{-# language DuplicateRecordFields #-}
{-# language MultiWayIf #-}
{-# language LambdaCase #-}
{-# language OverloadedRecordDot #-}
{-# language PatternSynonyms #-}

module Kafka.Unipartition.Leader
  ( Environment
  , Membership(..)
  , Stability(..)
  , Metadata(..)
    -- * Course grained
  , leadForever
    -- * Fine grained
  , connect
  , attemptLead
  , heartbeat
  ) where

import Kafka.Unipartition.Common

import Channel.Error (Error(Error))
import Channel.Error (Role(BootstrapServer,GroupCoordinator,PartitionHost))
import Control.Concurrent (threadDelay)
import Control.Monad (join,when,forever)
import Control.Monad.ST.Run (runByteArrayST)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT(ExceptT),runExceptT,throwE)
import Data.Bifunctor (first)
import Data.Bytes.Types (Bytes(Bytes))
import Data.Foldable (foldl',for_,find)
import Data.IORef (IORef,readIORef,writeIORef,newIORef)
import Data.Int (Int16,Int32,Int64)
import Data.Primitive (SmallArray,PrimArray)
import Data.Primitive.PrimVar (PrimVar,newPrimVar)
import Data.Text (Text)
import Data.Text.Short (ShortText)
import Data.WideWord (Word128)
import Data.Word (Word16)
import Kafka.ApiKey (ApiKey)
import Kafka.ErrorCode (pattern None,pattern RebalanceInProgress)
import Kafka.Record.Response (Record)
import Kafka.RecordBatch.Response (RecordBatch)
import ResolveHostname (query)
import Socket.Stream.IPv4 (Peer)

import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Bytes as Bytes
import qualified Data.Text.Short as TS
import qualified Kafka.JoinGroup.Request.V9 as JoinGroup.Request
import qualified Kafka.JoinGroup.Response.V9 as JoinGroup.Response
import qualified Data.Primitive.ByteArray.BigEndian as BigEndian
import qualified Socket.Stream.IPv4 as S
import qualified Channel.Error as Error
import qualified Channel.SocketAlt as A
import qualified Data.List as List
import qualified Data.Primitive as PM
import qualified Data.Primitive.Contiguous as Contiguous
import qualified Data.Text as T
import qualified Kafka.ApiKey as ApiKey
import qualified Kafka.SyncGroup.Request.V5 as SyncGroup.Request
import qualified Kafka.SyncGroup.Response.V5 as SyncGroup.Response
import qualified Kafka.ApiVersions.Request.V3 as ApiVersions.Request
import qualified Kafka.ApiVersions.Response.V3 as ApiVersions.Response
import qualified Kafka.OffsetFetch.Request.V8 as OffsetFetch.Request
import qualified Kafka.OffsetFetch.Response.V8 as OffsetFetch.Response
import qualified Kafka.Subscription.Request.V1 as Subscription.Request
import qualified Kafka.Subscription.Response.V1 as Subscription.Response
import qualified Kafka.Fetch.Request.V13 as Fetch.Request
import qualified Kafka.Fetch.Response.V13 as Fetch.Response
import qualified Kafka.Record.Response as Record
import qualified Kafka.RecordBatch.Attributes as Attributes
import qualified Kafka.RecordBatch.Response as RecordBatch
import qualified Socket.Stream.Kafka.ExchangeAlt as K
import qualified Kafka.Metadata.Request.V12 as Metadata.Request
import qualified Kafka.Metadata.Response.V12 as Metadata.Response
import qualified Kafka.FindCoordinator.Request.V4 as FindCoordinator.Request
import qualified Kafka.FindCoordinator.Response.V4 as FindCoordinator.Response
import qualified Kafka.Assignment.Response.V1 as Assignment.Response
import qualified Kafka.Assignment.Request.V1 as Assignment.Request
import qualified Kafka.LeaveGroup.Request.V5 as LeaveGroup.Request
import qualified Kafka.LeaveGroup.Response.V5 as LeaveGroup.Response
import qualified Kafka.Heartbeat.Request.V4 as Heartbeat.Request
import qualified Kafka.Heartbeat.Response.V4 as Heartbeat.Response
import qualified Kafka.ListOffsets.Request.V7 as ListOffsets.Request
import qualified Kafka.ListOffsets.Response.V7 as ListOffsets.Response

data Environment = Environment
  { coordinator :: !A.Environment
  , consumerGroupName :: !Text
  , topicName :: !Text
  , groupInstanceId :: !Text
  }

newtype Metadata = Metadata
  { partitions :: PrimArray Int32
  }

-- | Establish a connection to the coordinator. This first connects to
-- a bootstrap server.
--
-- In addition to returning an environment, this returns metadata about
-- the topic. At the moment, this is just the partition indices. In theory,
-- we could just return the partition count instead, but I'm not certain
-- that Kafka makes the guarantee that partition indices are a contiguous
-- block of integers starting at zero.
--
-- The metadata happens to be available from several setup calls that we
-- already have to run. The example 'leadForever' function does not use
-- this metadata, but if you are forking a thread for each partition's
-- consumer, the returned metadata is a convenient way to learn about all
-- the partitions you need to consume from.
connect ::
     ShortText -- bootstrap server host name
  -> Word16 -- bootstrap server port
  -> Text -- consumer group name
  -> Text -- topic name
  -> IO (Either Error (Environment,Metadata))
connect host port consumerGroupName topicName = do
  let groupInstanceId = buildGroupInstanceId consumerGroupName fakePartitionIndex
  runExceptT (connectToCoordinator host port consumerGroupName topicName) >>= \case
    Left e -> pure (Left e)
    Right (envCoord,metaResp,_) ->
      case findImportantMetadataWithoutPartition topicName metaResp of
        Left e -> pure (Left (Error ApiKey.Metadata e))
        Right ImportantMetadata{partitions} -> pure $ Right
          ( Environment
            { coordinator = envCoord
            , consumerGroupName = consumerGroupName
            , topicName = topicName
            , groupInstanceId = groupInstanceId
            }
          , Metadata
            { partitions = partitions
            }
          )

-- | Attempt to join the group as the leader. If the coordinator does not
-- assign us the role of leader, we leave the group and return Nothing. We
-- must wait and try again in a few seconds. If the coordinator does assign
-- us the role of leader, we call SyncGroup and assign the other members
-- their roles.
-- On success, this return information about its own membership. This is
-- needed by the heartbeat calls.
attemptLead :: Environment -> IO (Either Error (Maybe Membership))
attemptLead env = runExceptT $ do
  joinResp <- twoPhaseJoin env.coordinator env.topicName env.consumerGroupName env.groupInstanceId fakePartitionIndex
  let memberId = joinResp.memberId
  let generationId = joinResp.generationId
  case PM.sizeofSmallArray joinResp.members of
    0 -> do
      -- This means that were were not chosen as the leader. We must be the
      -- leader, so we leave the group. The caller is responsible for waiting
      -- and trying again.
      resp <- ExceptT $ K.leaveGroupV5 env.coordinator LeaveGroup.Request.Request
        { groupId = env.consumerGroupName
        , members = Contiguous.singleton LeaveGroup.Request.Member
          { memberId = memberId
          , groupInstanceId = Just env.groupInstanceId
          , reason = T.pack "leader-only member is leaving because of non-leader role"
          }
        }
      -- TODO: Check the response to make sure it indicates that
      -- we actually left the group.
      pure Nothing
    _ -> do
      resp <- ExceptT $ K.syncGroupV5 env.coordinator SyncGroup.Request.Request
        { groupId = env.consumerGroupName
        , generationId = generationId
        , memberId = memberId
        , groupInstanceId = Just env.groupInstanceId
        , protocolType = protocolType
        , protocolName = protocolName
        , assignments = Contiguous.mapMaybe
          (\mbr ->
            -- Do not assign anything to ourself. Assign every subscriber
            -- the partition that it asked for.
            if | mbr.memberId /= memberId
               , Right subscription <- Subscription.Response.decode mbr.metadata
               , Bytes.length subscription.userData == 4
               , prt <- case subscription.userData of
                   Bytes arr off _ -> BigEndian.indexUnalignedByteArray arr off :: Int32
               , PM.sizeofSmallArray subscription.topics == 1
               , Contiguous.index subscription.topics 0 == env.topicName -> Just
                   SyncGroup.Request.Assignment
                     { memberId = mbr.memberId
                     , assignment = Chunks.concat $ Assignment.Request.toChunks Assignment.Request.Assignment
                       { assignedPartitions = Contiguous.singleton Assignment.Request.Ownership
                         { topic = env.topicName
                         , partitions = Contiguous.singleton prt
                         }
                       , userData = subscription.userData
                       }
                     }
               | otherwise -> Nothing
          ) joinResp.members
        }
      when (resp.errorCode /= None) $ do
        throwE Error{context=ApiKey.SyncGroup,message=Error.ErrorCode resp.errorCode}
      pure $ Just Membership
        { memberId = joinResp.memberId
        , generationId = joinResp.generationId
        }

-- | Join a consumer group and act as the leader. This runs forever until
-- an error is encountered.
--
-- This is an example of how to combine 'connect', 'attemptLead', and
-- 'heartbeat'. Users are expected to adapt this to add logging in their
-- own applications.
leadForever ::
     ShortText -- bootstrap server host name
  -> Word16 -- bootstrap server port
  -> Text -- consumer group name
  -> Text -- topic name
  -> IO (Either Error a)
leadForever host port consumerGroupName topicName = runExceptT $ do
  (env,_) <- ExceptT (connect host port consumerGroupName topicName)
  forever $ do
    let becomeLeaderAndAssign = do
          ExceptT (attemptLead env) >>= \case
            Nothing -> do 
              lift (threadDelay 2_000_000)
              becomeLeaderAndAssign
            Just mbr -> pure mbr
    mbr <- becomeLeaderAndAssign
    let heartbeatUntilRebalance = do
          lift (threadDelay 250_000)
          ExceptT (heartbeat env mbr) >>= \case
            Stable -> heartbeatUntilRebalance
            Rebalancing -> pure ()
    heartbeatUntilRebalance

data Membership = Membership
  { memberId :: !Text
  , generationId :: !Int32
  }

data Stability
  = Rebalancing
  | Stable

heartbeat :: Environment -> Membership -> IO (Either Error Stability)
heartbeat env mbr = do
  eresp <- K.heartbeatV4 env.coordinator Heartbeat.Request.Request
    { groupId = env.consumerGroupName
    , memberId = mbr.memberId
    , generationId = mbr.generationId
    , groupInstanceId = Just env.groupInstanceId
    }
  case eresp of
    Left err -> pure (Left err)
    Right resp -> case resp.errorCode of
      None -> pure (pure Stable)
      -- On a rebalance, we break out of the heartbeat loop and
      -- rejoin the group.
      RebalanceInProgress -> pure (pure Rebalancing)
      _ -> pure (Left Error{context=ApiKey.Heartbeat,message=Error.ErrorCode resp.errorCode})
  
buildFetchRequest ::
     Int32 -- session id
  -> Int32 -- session epoch
  -> SmallArray Fetch.Request.Topic
  -> Fetch.Request.Request
buildFetchRequest !sessionId !fetchSessionEpoch !topics = Fetch.Request.Request
  { replicaId = (-1)
  , maxWaitMilliseconds = 750
  , minBytes = 1
  , maxBytes = 52428800
  , isolationLevel = 0
  , sessionId=sessionId
  , sessionEpoch=fetchSessionEpoch
  , topics
  , rackId = T.empty
  }

-- If the argument is non-positive, then the result is not meaningful.
-- It's well defined, just not useful.
incrementFetchSessionEpoch :: Int32 -> Int32
incrementFetchSessionEpoch !i = if i == maxBound
  then 1
  else i + 1

buildGroupInstanceId :: Text -> Int32 -> Text
buildGroupInstanceId groupId partitionIndex = T.concat
  [ groupId
  , T.pack "-unipartition-"
  , T.pack (show partitionIndex)
  ]

protocolType :: Text
protocolType = T.pack "consumer"

protocolName :: Text
protocolName = T.pack "unipartition"

buildSyncGroup :: Text -> Text -> Text -> Text -> Int32 -> Int32 -> SyncGroup.Request.Request
buildSyncGroup topicName groupId memberId groupInstanceId partition !generationId= SyncGroup.Request.Request
  { groupId=groupId
  , groupInstanceId=Just groupInstanceId
  , generationId=generationId
  , memberId=memberId
  , protocolType=protocolType
  , protocolName=protocolName
  , assignments=Contiguous.empty
  }

fakePartitionIndex :: Int32
fakePartitionIndex = 9999999
