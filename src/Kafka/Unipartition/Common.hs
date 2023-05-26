{-# language DuplicateRecordFields #-}
{-# language MultiWayIf #-}
{-# language LambdaCase #-}
{-# language OverloadedRecordDot #-}

module Kafka.Unipartition.Common
  ( KeyVersion
  , ImportantMetadata(..)
  , twoPhaseJoin
  , connectToCoordinator
  , assertSupportedVersions
  , requiredVersions
  , establish
  , findImportantMetadata
  , findImportantMetadataWithoutPartition
  ) where

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

connectToCoordinator ::
     ShortText -- bootstrap server host name
  -> Word16 -- bootstrap server port
  -> Text -- consumer group name
  -> Text -- topic name
  -> ExceptT Error IO
       ( A.Environment
       , Metadata.Response.Response
       , FindCoordinator.Response.Coordinator
       )
connectToCoordinator host port consumerGroupName topicName = do
  conn <- establish BootstrapServer host port
  ident <- lift (newPrimVar 0)
  let env = A.Environment
        { correlationIdVar=ident
        , clientIdStatic=T.pack "haskell-kafka"
        , resourceStatic=conn
        }
  assertSupportedVersions requiredVersions env
  resp0 <- ExceptT $ K.metadataV12 env Metadata.Request.Request
    { topics=Just $ Contiguous.singleton Metadata.Request.Topic
        { id = 0
        , name = Just topicName
        }
    , allowAutoTopicCreation=False
    , includeTopicAuthorizedOperations=False
    }
  resp1 <- ExceptT $ K.findCoordinatorV4 env FindCoordinator.Request.Request
    { keyType = 0
    , coordinatorKeys = Contiguous.singleton consumerGroupName
    }
  when (PM.sizeofSmallArray resp1.coordinators == 0) $
    ExceptT (pure (Left Error{context=ApiKey.FindCoordinator,message=Error.MissingCoordinators}))
  when (PM.sizeofSmallArray resp1.coordinators > 1) $
    ExceptT (pure (Left Error{context=ApiKey.FindCoordinator,message=Error.TooManyCoordinators}))
  let coordinator = PM.indexSmallArray resp1.coordinators 0
  when (coordinator.errorCode /= 0) $
    ExceptT (pure (Left Error{context=ApiKey.FindCoordinator,message=Error.CoordinatorErrorCode coordinator.errorCode}))
  when (coordinator.port < 0 || coordinator.port >= 65536) $
    ExceptT (pure (Left Error{context=ApiKey.FindCoordinator,message=Error.CoordinatorPort}))
  () <- ExceptT $ fmap (first (Error ApiKey.FindCoordinator . Error.Disconnect)) (S.disconnect conn)
  envCoord <- do
    connCoord <- establish GroupCoordinator (TS.fromText coordinator.host) (fromIntegral coordinator.port)
    identCoord <- lift (newPrimVar 0)
    pure A.Environment
      { correlationIdVar=identCoord
      , clientIdStatic=T.pack "haskell-kafka"
      , resourceStatic=connCoord
      }
  assertSupportedVersions requiredVersions envCoord
  pure (envCoord,resp0,coordinator)

assertSupportedVersions :: [KeyVersion] -> A.Environment -> ExceptT Error IO ()
assertSupportedVersions versions env = do
  resp <- ExceptT $ K.apiVersionsV3 env $ ApiVersions.Request.Request
    { clientSoftwareName=T.pack "kafsay"
    , clientSoftwareVersion=T.pack "1.0"
    }
  for_ versions $ \KeyVersion{key,version} ->
    case List.find
        (\ApiVersions.Response.ApiKeyVersionSupport{apiKey=x,minVersion,maxVersion} ->
          key == x &&
          version >= minVersion
          && version <= maxVersion
        ) (resp.apiKeys :: SmallArray ApiVersions.Response.ApiKeyVersionSupport) of
      Just{} -> pure ()
      Nothing -> ExceptT $ pure $ Left $ Error
        { context = ApiKey.ApiVersions
        , message = Error.InsufficientVersionSupport key version
        }

-- Note: I'm not actually sure if the two-phase join is required when using
-- static group membership. Kafka's documentation is not on this, and
-- experimentally, the first join seems to succeed when group.instance.id
-- is present.
twoPhaseJoin ::
     A.Environment
  -> Text -- topic name
  -> Text -- cg name
  -> Text -- group instance id
  -> Int32 -- partition index, a fake one for the leader
  -> ExceptT Error IO JoinGroup.Response.Response
twoPhaseJoin envCoord topicName consumerGroupName groupInstanceId partitionIndex = do
  (memberId,mgenerationId,resp0) <- do
    resp0@JoinGroup.Response.Response{errorCode=errorCodeA,generationId,memberId,members} <- ExceptT $ K.joinGroupV9 envCoord
      $ buildJoinGroupRequest topicName consumerGroupName T.empty groupInstanceId partitionIndex
    when (T.null memberId) $
      ExceptT (pure (Left Error{context=ApiKey.JoinGroup,message=Error.NullMemberId}))
    case errorCodeA of
      -- Server boundes us and we have to make a second request.
      79 -> pure (memberId,Nothing,resp0)
      -- Added immidiately. No second request needed
      0 -> pure (memberId,Just generationId,resp0)
      -- A real error happened
      _ -> ExceptT (pure (Left Error{context=ApiKey.JoinGroup,message=Error.CoordinatorDidNotRequireMemberId errorCodeA memberId}))
  -- When mgenerationId is Nothing, we have not actually joined the group yet,
  -- and we have to make a second request.
  case mgenerationId of
    Just genId -> pure resp0
    Nothing -> do
      resp1@JoinGroup.Response.Response{errorCode=errorCodeB,generationId,members} <- ExceptT $ K.joinGroupV9 envCoord
        $ buildJoinGroupRequest topicName consumerGroupName memberId groupInstanceId partitionIndex
      when (errorCodeB /= 0) $
        ExceptT (pure (Left Error{context=ApiKey.JoinGroup,message=Error.CoordinatorErrorCode errorCodeB}))
      pure resp1

data KeyVersion = KeyVersion
  { key :: !ApiKey
  , version :: !Int16
  }
  
requiredVersions :: [KeyVersion]
requiredVersions =
  [ KeyVersion ApiKey.Metadata 12
  , KeyVersion ApiKey.ListOffsets 7
  , KeyVersion ApiKey.JoinGroup 9
  , KeyVersion ApiKey.SyncGroup 5
  , KeyVersion ApiKey.OffsetFetch 8 -- not actually used yet
  , KeyVersion ApiKey.Fetch 13
  , KeyVersion ApiKey.FindCoordinator 4
  ]

establish ::
     Role
  -> ShortText -- hostname
  -> Word16 -- port
  -> ExceptT Error IO S.Connection
establish role !host !port = do
  addr <- ExceptT (fmap (first (\_ -> Error ApiKey.ApiVersions (Error.HostnameResolution role host))) (query host))
  let peer = S.Peer
        { address = addr
        , port = port
        }
  ExceptT $ fmap (first (Error ApiKey.ApiVersions . Error.Connect)) (S.connect peer)

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

data ImportantMetadata = ImportantMetadata
  { host :: !Text
  , port :: !Word16
  , uuid :: !Word128
  , leaderEpoch :: !Int32
  , partitions :: !(PrimArray Int32)
  }

findImportantMetadata ::
     Text -- topic name
  -> Int32 -- partition that we are interested in
  -> Metadata.Response.Response
  -> Either Error.Message ImportantMetadata
findImportantMetadata !name !partitionIndex resp = do
  when (PM.sizeofSmallArray resp.topics < 1) (Left Error.ResponseMissingTopics)
  when (PM.sizeofSmallArray resp.topics > 1) (Left Error.ResponseTooManyTopics)
  let topic = PM.indexSmallArray resp.topics 0
  when (topic.errorCode /= 0) (Left (Error.ResponseTopicErrorCode topic.errorCode))
  when (topic.name /= name) (Left Error.UnexpectedTopicName)
  case find (\p -> p.index == partitionIndex) topic.partitions of
    Nothing -> Left Error.MissingPartition
    Just p -> case find (\b -> b.nodeId == p.leaderId) resp.brokers of
      Nothing -> Left (Error.MissingBroker p.leaderId)
      Just b
        | b.port < 0 || b.port >= 65536 -> Left Error.BrokerPort
        | otherwise -> Right ImportantMetadata
            { uuid=topic.id
            , host=b.host
            , port=fromIntegral b.port
            , leaderEpoch=p.leaderEpoch
            , partitions=Contiguous.map' (\p -> p.index) topic.partitions
            }

findImportantMetadataWithoutPartition ::
     Text -- topic name
  -> Metadata.Response.Response
  -> Either Error.Message ImportantMetadata
findImportantMetadataWithoutPartition !name resp = do
  when (PM.sizeofSmallArray resp.topics < 1) (Left Error.ResponseMissingTopics)
  when (PM.sizeofSmallArray resp.topics > 1) (Left Error.ResponseTooManyTopics)
  let topic = PM.indexSmallArray resp.topics 0
  when (topic.errorCode /= 0) (Left (Error.ResponseTopicErrorCode topic.errorCode))
  when (topic.name /= name) (Left Error.UnexpectedTopicName)
  Right ImportantMetadata
    { uuid=topic.id
    , host=mempty
    , port=0
    , leaderEpoch=0
    , partitions=Contiguous.map' (\p -> p.index) topic.partitions
    }
