{-# language LambdaCase #-}
{-# language DeriveFunctor #-}
{-# language DerivingStrategies #-}

module Channel.Socket
  ( M
  , run
  , clientId
  , nextCorrelationId
  , throw
  , send
  , receiveExactly
  ) where

import Data.Int (Int32)
import Data.Text (Text)
import Socket.Stream.IPv4 (Connection)
import Kafka.Exchange.Error (Error)
import Data.Bytes.Chunks (Chunks)
import Data.Primitive (ByteArray)
import Control.Monad.IO.Class (MonadIO(liftIO))

import qualified Socket.Stream.Uninterruptible.Bytes as Stream
import qualified Data.Bytes.Chunks as Chunks

newtype M a = M
  (Connection -> Text -> Int32 -> IO (Either Error (Output a)))
  deriving stock (Functor)

-- | Run a sequence of actions that communicate with Apache Kafka.
-- This uses zero as the initial correlation ID and does not
-- return the final correlation ID to the caller.
run ::
     Connection -- ^ Connection to Kafka broker
  -> Text -- ^ Client ID
  -> M a -- ^ Action
  -> IO (Either Error a)
run !conn !clientId (M f) = f conn clientId 0 >>= \case
  Left e -> pure (Left e)
  Right (Output _ a) -> pure (Right a)

data Output a = Output
  !Int32 -- correlation id
  a
  deriving stock (Functor)

instance Applicative M where
  pure = pureM
  f <*> a = f `bindM` \f' -> a `bindM` \a' -> pureM (f' a')

instance Monad M where
  (>>=) = bindM

instance MonadIO M where
  liftIO m = M $ \_ _ z -> do
    a <- m
    pure (Right (Output z a))

pureM :: a -> M a
pureM x = M $ \_ _ c -> pure (Right (Output c x))

bindM :: M a -> (a -> M b) -> M b
bindM (M f) g = M $ \x y z -> f x y z >>= \case
  Left e -> pure (Left e)
  Right (Output z' r) -> case g r of
    M k -> k x y z'

clientId :: M Text
clientId = M (\_ x c -> pure (pure (Output c x)))

-- When the correlation id overflows, this sets it to zero
-- rather than a high-magnitude negative integer.
nextCorrelationId :: M Int32
nextCorrelationId = M (\_ _ c -> pure (pure (Output (max 0 (c+1)) c)))

throw :: Error -> M a
throw e = M (\_ _ _ -> pure (Left e))

send :: Error -> Chunks -> M ()
{-# inline send #-}
send e ch = M $ \conn _ c -> Stream.send conn (Chunks.concat ch) >>= \case
  Left{} -> pure (Left e)
  Right{} -> pure (Right (Output c ()))

receiveExactly :: Error -> Int -> M ByteArray
{-# inline receiveExactly #-}
receiveExactly e n = M $ \conn _ c -> Stream.receiveExactly conn n >>= \case
  Left{} -> pure (Left e)
  Right a -> pure (Right (Output c a))
