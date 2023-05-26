{-# language BangPatterns #-}
{-# language DataKinds #-}

module Channel.Error
  ( Error(..)
  , Message(..)
  , Role(..)
  , send
  , receiveLength
  , receiveBody
  , responseBodyMalformed
  , responseHeaderIncorrectCorrelationId
  , responseHeaderMalformed
  , responseLengthNegative
  , responseLengthTooHigh
  ) where

import Control.Exception (Exception)
import Kafka.ApiKey (ApiKey)
import Data.Int (Int16,Int32)
import Data.Text (Text)
import Data.Text.Short (ShortText)
import Socket.Stream.IPv4 (CloseException,ConnectException,SendException,ReceiveException,Interruptibility(Uninterruptible),Family(Internet),Version(V4))

data Error = Error
  { context :: !ApiKey
    -- ^ What kind of request to Kafka led to a problem.
  , message :: !Message
  } deriving (Show)

instance Exception Error

data Message
  = ResponseBodyMalformed
  | ResponseHeaderIncorrectCorrelationId
  | ResponseLengthNegative
  | ResponseLengthTooHigh
  | ResponseHeaderMalformed
  | Connect !(ConnectException ('Internet 'V4) 'Uninterruptible)
  | Disconnect !CloseException
  | Send !(SendException 'Uninterruptible)
  | ReceiveLength !(ReceiveException 'Uninterruptible)
  | ReceiveBody !(ReceiveException 'Uninterruptible)
  | IncorrectSessionId
  | ResponseErrorCode !Int16
  | PartitionErrorCode !Int16
  | ResponseTooManyPartitions
  | ResponseTooManyTopics
  | ResponseMissingTopics
  | TooManyCoordinators
  | MissingCoordinators
  | CoordinatorPort
    -- ^ The coordinator uses a port that is outside of the 16-bit port range
  | ResponseTopicErrorCode !Int16
  | CoordinatorErrorCode !Int16
  | MalformedRecords
  | MissingLatestOffset
  | MissingEarliestOffset
  | UnsupportedCompressionScheme
  | UnexpectedPartitionAssignment
      !Int32 -- assigned partition
  | UnexpectedTopicAssignment
  | MalformedAssignment
  | SelfGroupLeader
  | SelfNotGroupLeader
  | UnexpectedGroupProtocol
  | SyncGroupErrorCode
  | AssignmentTopicCount
  | AssignmentPartitionCount
  | CoordinatorDidNotRequireMemberId
      !Int16 -- error code
      !Text -- member id returned by coordinator
  | NullMemberId
  | MissingBroker !Int32
  | BrokerPort
  | MissingPartition
  | UnexpectedTopicName
  | NegativeCommittedOffset
  | FetchSessionIdZero
  | UnexpectedPartition
  | UnexpectedPartitionCount
  | UnexpectedTopicCount
  | UnexpectedGroupCount
  | ErrorCode !Int16
  | HostnameResolution
      Role
      !ShortText -- hostname that could not be resolved
  | InsufficientVersionSupport
      !ApiKey
      !Int16 -- version we wanted to use
  deriving (Show)

data Role
  = GroupCoordinator
  | BootstrapServer
  | PartitionHost
  deriving (Show)

-- Failure to send a message. 
send :: ApiKey -> SendException 'Uninterruptible -> Error
send k e = Error{context=k,message=Send e}

-- Failures that happen as a response is being received.
-- There is some underlying error that the kernel tells
-- us about (usually that the other end shut down the
-- connection), and we can wrap this up and include it
-- in the error message to the user.
receiveLength :: ApiKey -> ReceiveException 'Uninterruptible -> Error
receiveLength k e = Error{context=k,message=ReceiveLength e}

receiveBody :: ApiKey -> ReceiveException 'Uninterruptible -> Error
receiveBody k e = Error{context=k,message=ReceiveBody e}

-- Failure that happen where we are processing data that
-- was received. This includes problems parsing the payload
-- and problems with the payload not making sense.
responseLengthNegative :: ApiKey -> Error
responseLengthNegative k = Error{context=k,message=ResponseLengthNegative}
responseLengthTooHigh :: ApiKey -> Error
responseLengthTooHigh k = Error{context=k,message=ResponseLengthTooHigh}
responseHeaderMalformed :: ApiKey -> Error
responseHeaderMalformed k = Error{context=k,message=ResponseHeaderMalformed}
responseHeaderIncorrectCorrelationId :: ApiKey -> Error
responseHeaderIncorrectCorrelationId k = Error{context=k,message=ResponseHeaderIncorrectCorrelationId}
responseBodyMalformed :: ApiKey -> Error
responseBodyMalformed k = Error{context=k,message=ResponseBodyMalformed}
