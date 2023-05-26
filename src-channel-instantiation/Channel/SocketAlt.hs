{-# language LambdaCase #-}
{-# language DeriveFunctor #-}
{-# language DerivingStrategies #-}
{-# language DataKinds #-}

module Channel.SocketAlt
  ( M
  , ReceiveError
  , SendError
  , Resource
  , Environment(..)
  , clientId
  , resource
  , nextCorrelationId
  , send
  , Stream.receiveExactly
  ) where

import Data.Int (Int32)
import Data.Text (Text)
import Socket.Stream.IPv4 (Connection)
import Data.Bytes.Chunks (Chunks)
import Data.Primitive (ByteArray)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Primitive.PrimVar
import GHC.Exts (RealWorld)

import qualified Kafka.Exchange.Error
import qualified Socket.Stream.Uninterruptible.Bytes as Stream
import qualified Socket.Stream.IPv4 as Stream
import qualified Data.Bytes.Chunks as Chunks

type M = IO

type ReceiveError = Stream.ReceiveException 'Stream.Uninterruptible
type SendError = Stream.SendException 'Stream.Uninterruptible
type Resource = Stream.Connection

data Environment = Environment
  { correlationIdVar :: !(PrimVar RealWorld Int32)
  , clientIdStatic :: !Text
  , resourceStatic :: !Stream.Connection
  }

clientId :: Environment -> Text
clientId Environment{clientIdStatic} = clientIdStatic

resource :: Environment -> Resource
resource Environment{resourceStatic} = resourceStatic

-- When the correlation id overflows, this sets it to zero
-- rather than a high-magnitude negative integer.
nextCorrelationId :: Environment -> IO Int32
nextCorrelationId Environment{correlationIdVar} = do
  v <- readPrimVar correlationIdVar
  writePrimVar correlationIdVar (max 0 (v + 1))
  pure v

send :: Connection -> Chunks -> IO (Either (Stream.SendException 'Stream.Uninterruptible) ())
{-# inline send #-}
send conn ch = Stream.send conn (Chunks.concat ch)
-- 
-- receiveExactly :: Error -> Int -> M ByteArray
-- {-# inline receiveExactly #-}
-- receiveExactly e n = M $ \conn _ c -> Stream.receiveExactly conn n >>= \case
--   Left{} -> pure (Left e)
--   Right a -> pure (Right (Output c a))
