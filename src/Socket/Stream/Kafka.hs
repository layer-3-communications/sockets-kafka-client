module Socket.Stream.Kafka
  ( -- * Monadic Interface
    M
  , run
  , throw
    -- * Exchanges
  , X.apiVersionsV3
  , X.fetchV13
  , X.findCoordinatorV4
  , X.initProducerIdV4
  , X.joinGroupV9
  , X.listOffsetsV7
  , X.metadataV12
  , X.produceV9
  , X.syncGroupV5
  ) where

-- Note: We could instead do a module reexport for
-- Socket.Stream.Kafka.Exchange. However, this leads to bad
-- haddocks. Instead, we enumerate the functions, one by one,
-- which causes them to show up in the haddocks.

import Channel.Socket (M,run,throw)
import Socket.Stream.Kafka.Exchange as X
