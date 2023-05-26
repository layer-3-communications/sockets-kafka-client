{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language PatternSynonyms #-}
{-# language TypeApplications #-}

module ResolveHostname
  ( query
  ) where

import Foreign.Ptr (Ptr,nullPtr,castPtr)
import Posix.Socket (SocketAddressInternet,SocketAddress,AddressInfo)
import Posix.Socket (pattern Internet)
import System.ByteOrder (Fixed(Fixed))
import Foreign.C.String.Managed (ManagedCString)
import Net.Types (IPv4(IPv4))
import Data.Text.Short (ShortText)
import qualified Data.Bytes as Bytes
import qualified Data.Text.Short as TS
import qualified Foreign.C.String.Managed as MCS
import qualified Posix.Struct.SocketAddressInternet.Peek as SAI
import qualified Posix.Struct.AddressInfo.Peek as AI
import qualified Posix.Socket as S

query :: ShortText -> IO (Either () IPv4)
query !host = lookupPinned (MCS.pinnedFromBytes b)
  where
  b = Bytes.fromShortByteString (TS.toShortByteString host)

lookupPinned :: ManagedCString -> IO (Either () IPv4)
lookupPinned host = do
  S.getAddressInfo (MCS.contents host) nullPtr nullPtr >>= \case
    Left _ -> do
      MCS.touch host
      pure (Left ())
    Right info -> do
      MCS.touch host
      r <- extractAddressLoop info
      S.uninterruptibleFreeAddressInfo info
      pure r

-- Only uses IO for peeking. 
extractAddressLoop :: Ptr AddressInfo -> IO (Either () IPv4)
extractAddressLoop info = do
  AI.family info >>= \case
    Internet -> do
      sockAddr <- AI.address info
      let sai = castPtr @SocketAddress @SocketAddressInternet sockAddr
      Fixed address <- SAI.address sai
      pure (Right (IPv4 address))
    _ -> do
      infoSucc <- AI.next info
      if infoSucc == nullPtr
        then pure (Left ())
        else extractAddressLoop infoSucc

