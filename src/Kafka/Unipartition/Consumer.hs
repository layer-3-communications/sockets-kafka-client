{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language MultiWayIf #-}
{-# language OverloadedRecordDot #-}
{-# language PatternSynonyms #-}

module Kafka.Unipartition.Consumer
  ( Environment
  , connect
  , fetch
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
import Data.Primitive (SmallArray)
import Data.Primitive.PrimVar (PrimVar,newPrimVar)
import Data.Text (Text)
import Data.Text.Short (ShortText)
import Data.WideWord (Word128)
import Data.Word (Word16)
import Kafka.ApiKey (ApiKey)
import Kafka.ErrorCode (pattern None,pattern OffsetOutOfRange)
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
  , broker :: !A.Environment
  , memberId :: !Text
  , topicName :: !Text
  , topicUuid :: {-# UNPACK #-} !Word128
  , partitionIndex :: !Int32
  , partitionLeaderEpoch :: !Int32
  , fetchSessionId :: !Int32
  , status :: !(IORef Status)
  }

data Status
  = Healthy !State
  | Unhealthy

data State = State
  { fetchSessionEpoch :: !Int32
    -- ^ Session epoch
  , offset :: !Int64
  , repetition :: !Bool
    -- ^ Is this a repeat of the previous fetch request? If it is,
    -- then we do not need to send a list of partitions.
  , lastFetchedEpoch :: !Int32
    -- ^ Kafka has some trick for figuring out if epoch divergence has
    -- happened, and we have to supply the greatest epoch from our most
    -- recent fetch to help it do this. 
  }

-- | Connects first to a bootstrap server, then to the group coordinator,
-- and then finally to the broker that hosts the partition.
connect ::
     ShortText -- bootstrap server host name
  -> Word16 -- bootstrap server port
  -> Text -- consumer group name
  -> Text -- topic name
  -> Int32 -- partition
  -> IO (Either Error Environment)
connect host port consumerGroupName topicName !partitionIndex = runExceptT $ do
  (envCoord,metaResp,coordinator) <- connectToCoordinator host port consumerGroupName topicName
  ImportantMetadata{uuid=topicUuid,host=prtHost,port=prtPort,leaderEpoch} <- ExceptT $ pure
    $ first (Error ApiKey.Metadata) (findImportantMetadata topicName partitionIndex metaResp)
  let groupInstanceId = buildGroupInstanceId consumerGroupName partitionIndex
  joinResp <- twoPhaseJoin envCoord topicName consumerGroupName groupInstanceId partitionIndex
  let memberId = joinResp.memberId
  let generationId = joinResp.generationId
  when (PM.sizeofSmallArray joinResp.members /= 0) $ do
    -- This means that were were chosen as the leader. We do not want to
    -- be the leader, so we leave the group and indicate failure.
    _ <- ExceptT $ K.leaveGroupV5 envCoord LeaveGroup.Request.Request
      { groupId = consumerGroupName
      , members = Contiguous.singleton LeaveGroup.Request.Member
        { memberId = memberId
        , groupInstanceId = Just groupInstanceId
        , reason = T.pack "follower-only member is leaving because of leader role"
        }
      }
    -- TODO: check the response
    ExceptT (pure (Left Error{context=ApiKey.JoinGroup,message=Error.SelfGroupLeader}))
  () <- do
    resp <- ExceptT $ K.syncGroupV5 envCoord
      $ buildSyncGroup topicName consumerGroupName memberId groupInstanceId partitionIndex generationId
    when (resp.errorCode /= None) $
      ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.SyncGroupErrorCode}))
    when (resp.protocolName /= protocolName) $
      ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.UnexpectedGroupProtocol}))
    case Assignment.Response.decode resp.assignment of
      Left _ -> ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.MalformedAssignment}))
      Right assgn -> do
        let ownerships = assgn.assignedPartitions
        when (PM.sizeofSmallArray ownerships /= 1) $ do
          ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.AssignmentTopicCount}))
        let Assignment.Response.Ownership{topic,partitions} = PM.indexSmallArray ownerships 0
        when (topic /= topicName) $ do
          ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.UnexpectedTopicAssignment}))
        when (PM.sizeofPrimArray partitions /= 1) $ do
          ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.AssignmentPartitionCount}))
        let prt = PM.indexPrimArray partitions 0
        when (prt /= partitionIndex) $ do
          ExceptT (pure (Left Error{context=ApiKey.SyncGroup,message=Error.UnexpectedPartitionAssignment prt}))
  -- If the coordinating broker and the partition-hosting broker are the
  -- same, then we reuse a single TCP connection rather than opening
  -- a second one. Then means that we have to be careful when closing
  -- the TCP session.
  envBroker <- if coordinator.host == prtHost && fromIntegral @Int32 @Word16 coordinator.port == prtPort
    then pure envCoord
    else do
      conn <- establish PartitionHost (TS.fromText prtHost) prtPort
      ident <- lift (newPrimVar 0)
      let env = A.Environment
            { correlationIdVar=ident
            , clientIdStatic=T.pack "haskell-kafka"
            , resourceStatic=conn
            }
      assertSupportedVersions requiredVersions env
      pure env
  initialOffset <- do
    resp <- ExceptT $ K.offsetFetchV8 envCoord OffsetFetch.Request.Request
      { groups=Contiguous.singleton OffsetFetch.Request.Group
        { id=consumerGroupName
        , topics=Contiguous.singleton OffsetFetch.Request.Topic
          { name=topicName
          , partitions=Contiguous.singleton partitionIndex
          }
        }
      , requireStable = True
      }
    when (PM.sizeofSmallArray resp.groups /= 1) $
      ExceptT (pure (Left Error{context=ApiKey.OffsetFetch,message=Error.UnexpectedGroupCount}))
    let group = PM.indexSmallArray resp.groups 0
    when (PM.sizeofSmallArray group.topics /= 1) $
      ExceptT (pure (Left Error{context=ApiKey.OffsetFetch,message=Error.UnexpectedTopicCount}))
    let topic = PM.indexSmallArray group.topics 0
    when (PM.sizeofSmallArray topic.partitions /= 1) $
      ExceptT (pure (Left Error{context=ApiKey.OffsetFetch,message=Error.UnexpectedPartitionCount}))
    let prt = PM.indexSmallArray topic.partitions 0
    when (prt.index /= partitionIndex) $
      throwE Error{context=ApiKey.OffsetFetch,message=Error.UnexpectedPartition}
    earliest <- getSingleOffset (-2) partitionIndex leaderEpoch topicName envBroker
    latest <- getSingleOffset (-1) partitionIndex leaderEpoch topicName envBroker
    if | prt.committedOffset == (-1) -> do
           -- If there is no committed offset, we start at the end.
           pure latest
       | prt.committedOffset >= 0 -> if prt.committedOffset >= earliest
           then pure prt.committedOffset
           -- Jump to the end if the committed offset points a record
           -- that has already aged out.
           else pure latest
       | otherwise -> ExceptT (pure (Left Error{context=ApiKey.OffsetFetch,message=Error.NegativeCommittedOffset}))
  -- This initial fetch is only used to create a session id. All the data
  -- is thrown away. Although this is wasteful, it simplifies the consumer.
  -- Kafka responds differently to fetches that use a session id when no
  -- records are available.
  fetchResp <- ExceptT $ K.fetchV13 envBroker $ buildFetchRequest 0 0 $
    Contiguous.singleton Fetch.Request.Topic
      { id = topicUuid
      , partitions = Contiguous.singleton Fetch.Request.Partition
        { index = partitionIndex
        , currentLeaderEpoch = leaderEpoch
        , fetchOffset = initialOffset
        , lastFetchedEpoch = (-1)
        , logStartOffset = (-1)
        , maxBytes = 1048576
        }
      }
  when (fetchResp.errorCode /= None && fetchResp.errorCode /= OffsetOutOfRange) (throwE Error{context=ApiKey.Fetch,message=Error.ErrorCode fetchResp.errorCode})
  when (fetchResp.sessionId == 0) (throwE Error{context=ApiKey.Fetch,message=Error.FetchSessionIdZero})
  statusRef <- lift $ newIORef $! Healthy $! State
    { fetchSessionEpoch = 1
    , offset = initialOffset
    , repetition = False
    , lastFetchedEpoch = (-1)
    }
  pure $! Environment
    { coordinator = envCoord
    , broker = envBroker
    , memberId = memberId
    , topicName = topicName
    , topicUuid = topicUuid
    , partitionIndex = partitionIndex
    , partitionLeaderEpoch = leaderEpoch
    , fetchSessionId = fetchResp.sessionId
    , status = statusRef
    }

-- commit ::
--      Environment
--   -> Int64 -- offset
--   -> IO ()
-- commit = _

-- | Fetch records from the partition. The timestamps and offsets are
-- absolute, not relative.
-- If this ever returns an error, do not keep calling fetch. It's not
-- going to start working again.
fetch :: Environment -> IO (Either Error (SmallArray Record))
fetch e = do
  readIORef e.status >>= \case
    Unhealthy -> fail "Kafka consumer: cannot call fetch on degraded session"
    Healthy s -> do
      when s.repetition (threadDelay 250_000)
      let finishWithError err = do
            writeIORef e.status Unhealthy
            pure $ Left Error
              { context=ApiKey.Fetch
              , message=err
              }
      let finishNoRecords = do
            writeIORef e.status $! Healthy $! State
              { fetchSessionEpoch = incrementFetchSessionEpoch s.fetchSessionEpoch
              , offset = s.offset
              , repetition = True
              , lastFetchedEpoch = s.lastFetchedEpoch
              }
            pure (Right mempty)
      let topics = if s.repetition
            then mempty
            else Contiguous.singleton (buildTopicRequest e s)
      K.fetchV13 e.broker (buildFetchRequest e.fetchSessionId s.fetchSessionEpoch topics) >>= \case
        Left err -> pure (Left err)
        Right resp
          | resp.sessionId /= e.fetchSessionId -> finishWithError Error.IncorrectSessionId
          | resp.errorCode /= None -> finishWithError (Error.ResponseErrorCode resp.errorCode)
          | PM.sizeofSmallArray resp.topics >= 2 -> finishWithError Error.ResponseTooManyTopics
          | PM.sizeofSmallArray resp.topics == 0 -> finishNoRecords
          | Fetch.Response.Topic{partitions} <- PM.indexSmallArray resp.topics 0 ->
              if | PM.sizeofSmallArray partitions == 0 -> finishNoRecords
                 | PM.sizeofSmallArray partitions >= 2 -> finishWithError Error.ResponseTooManyPartitions
                 | prt <- PM.indexSmallArray partitions 0 ->
                     if | prt.errorCode /= None -> finishWithError (Error.PartitionErrorCode prt.errorCode)
                        | otherwise -> case traverse recordBatchToRecords prt.records of
                            Left err -> finishWithError err
                            Right xs -> do
                              writeIORef e.status $! Healthy $! State
                                { fetchSessionEpoch = incrementFetchSessionEpoch s.fetchSessionEpoch
                                , offset = maxOffsetInBatches prt.records + 1
                                , repetition = False
                                , lastFetchedEpoch = maxLeaderEpochInBatches prt.records
                                }
                              pure $! Right $! join xs

recordBatchToRecords :: RecordBatch -> Either Error.Message (SmallArray Record)
recordBatchToRecords b
  | Attributes.getCompression b.attributes /= Attributes.None =
      Left Error.UnsupportedCompressionScheme
  | Just records <- Record.decodeArrayAbsolute b.baseTimestamp b.baseOffset b.recordsPayload = Right records
  | otherwise =  Left Error.MalformedRecords

maxLeaderEpochInBatches :: SmallArray RecordBatch -> Int32
maxLeaderEpochInBatches = foldl'
  (\acc r -> max r.partitionLeaderEpoch acc
  ) (-1)

maxOffsetInBatches :: SmallArray RecordBatch -> Int64
maxOffsetInBatches = foldl'
  (\acc r -> max (r.baseOffset + fromIntegral r.lastOffsetDelta) acc
  ) (-1)
          
countRecordsInBatches :: SmallArray RecordBatch -> Int
countRecordsInBatches = foldl'
  (\acc r -> fromIntegral r.recordsCount + acc
  ) 0

-- This does not use fetch session information from the environment
buildTopicRequest ::
     Environment
  -> State
  -> Fetch.Request.Topic
buildTopicRequest e s = Fetch.Request.Topic
  { id = e.topicUuid
  , partitions = Contiguous.singleton $ Fetch.Request.Partition
    { index = e.partitionIndex
    , currentLeaderEpoch = e.partitionLeaderEpoch
    , fetchOffset = s.offset
    , lastFetchedEpoch = s.lastFetchedEpoch
    , logStartOffset = (-1)
    , maxBytes = 1048576
    }
  }

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

buildJoinGroupRequest :: Text -> Text -> Text -> Text -> Int32 -> JoinGroup.Request.Request
buildJoinGroupRequest topicName groupId memberId groupInstanceId !partitionIndex = JoinGroup.Request.Request
  { groupId=groupId
  , groupInstanceId=Just groupInstanceId
    -- ^ This uses static group membership.
  , memberId=memberId
  , sessionTimeoutMilliseconds=45000
  , rebalanceTimeoutMilliseconds=300000
  , protocolType=protocolType
  , protocols=Contiguous.singleton $ JoinGroup.Request.Protocol
    { name=protocolName
    , metadata=Chunks.concat $ Subscription.Request.toChunks Subscription.Request.Subscription
      { topics=Contiguous.singleton topicName
      , userData=Bytes.fromByteArray $ runByteArrayST $ do
          -- The user data is just the partition. The consumer group leader
          -- uses this to assign the member the partition it requested.
          dst <- PM.newByteArray 4
          BigEndian.writeByteArray dst 0 partitionIndex
          PM.unsafeFreezeByteArray dst
      , ownedPartitions=mempty
      }
    }
  , reason=T.empty
  }

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

getSingleOffset ::
     Int64 -- timestamp
  -> Int32 -- partition
  -> Int32 -- leader epoch
  -> Text -- topic name
  -> A.Environment
  -> ExceptT Error IO Int64
getSingleOffset !timestamp !partitionIndex !leaderEpoch !topicName env = do
  resp <- ExceptT $ K.listOffsetsV7 env ListOffsets.Request.Request
    { replicaId=(-1)
    , isolationLevel=0
    , topics=Contiguous.singleton ListOffsets.Request.Topic
      { name=topicName
      , partitions=Contiguous.singleton ListOffsets.Request.Partition
        { index=partitionIndex
        , currentLeaderEpoch=leaderEpoch
        , timestamp=timestamp
        }
      }
    }
  when (PM.sizeofSmallArray resp.topics /= 1) $
    ExceptT (pure (Left Error{context=ApiKey.ListOffsets,message=Error.UnexpectedTopicCount}))
  let topic = PM.indexSmallArray resp.topics 0
  when (PM.sizeofSmallArray topic.partitions /= 1) $
    ExceptT (pure (Left Error{context=ApiKey.ListOffsets,message=Error.UnexpectedPartitionCount}))
  let partition = PM.indexSmallArray topic.partitions 0
  pure partition.offset

