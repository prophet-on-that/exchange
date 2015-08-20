{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Exchange where

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map
import qualified Focus
import Control.Concurrent.STM
import Control.Lens
import Data.Time
import TH_Utils
import Control.Monad
import Data.Foldable
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Ord

type Price = Integer

declareLensesWith unprefixedFields [d|
  data Bid = Bid
    { clientRef :: !Integer
    , bidId :: !Integer
    , quantity :: !Integer
    , maximumPrice :: !Price
    , timestamp :: {-# UNPACK #-} !UTCTime
    } deriving (Eq)
  |]

instance Ord Bid where
  compare
    = comparing (view bidId)

declareLensesWith unprefixedFields [d|
  data Offer = Offer
    { clientRef :: !Integer
    , offerId :: !Integer
    , quantity :: !Integer
    , minimumPrice :: !Price
    , timestamp :: {-# UNPACK #-} !UTCTime
    } deriving (Eq)
  |]

instance Ord Offer where
  compare
    = comparing (view offerId)

declareLensesWith unprefixedFields [d|
  data Quotes = Quotes
    { bids :: !(Set Bid)
    , offers :: !(Set Offer)
    } 
  |]
 
emptyQuotes :: Quotes
emptyQuotes
  = Quotes Set.empty Set.empty

declareLensesWith unprefixedFields [d|
  data Exchange = Exchange
    { book :: Map Price Quotes
    , bestBid :: TVar Price -- ^ The best bid is derived from the book, we store it here to avoid needless recomputation.
    , bestOffer :: TVar Price -- ^ The best offer is derived from the book, we store it here to avoid needless recomputation.
    }
  |]

-- | Install 'Bid's into the book. 
addBids
  :: [Bid]
  -> Exchange
  -> STM ()
addBids bids' exch = do
  bestOffer' <- readTVar $ view bestOffer exch
  let
    updateMap :: Maybe Price -> Bid -> STM (Maybe Price)
    updateMap highestBid bid = do
      let
        bidPrice
          = min (view maximumPrice bid) bestOffer'
            
        alter :: Maybe Quotes -> STM (Maybe Quotes)
        alter
          = return . Just . modQuotes . fromMaybe emptyQuotes
          where
            modQuotes
              = bids %~ Set.insert bid
            
      Map.focus (Focus.alterM alter) bidPrice (view book exch)
      return $ max (Just bidPrice) highestBid
      
  highestBid <- foldlM updateMap Nothing bids'

  -- Update stored best bid.
  case highestBid of
    Nothing ->
      return ()
    Just highestBid' -> do
      bestBid' <- readTVar $ view bestBid exch
      when (highestBid' > bestBid') $
        writeTVar (view bestBid exch) highestBid'

-- | Install 'Offer's into the book. 
addOffers
  :: [Offer]
  -> Exchange
  -> STM ()
addOffers offers' exch = do
  bestBid' <- readTVar $ view bestBid exch
  let
    updateMap :: Maybe Price -> Offer -> STM (Maybe Price)
    updateMap lowestOffer offer = do
      let
        offerPrice
          = max (view minimumPrice offer) bestBid'
            
        alter :: Maybe Quotes -> STM (Maybe Quotes)
        alter 
          = return . Just . modQuotes . fromMaybe emptyQuotes
          where
            modQuotes
              = offers %~ Set.insert offer
            
      Map.focus (Focus.alterM alter) offerPrice (view book exch)
      case lowestOffer of
        Nothing ->
          return $ Just offerPrice
        (Just lowestOffer') ->
          return . Just $ min offerPrice lowestOffer'
      
  lowestOffer <- foldlM updateMap Nothing offers'

  -- Update stored best offer.
  case lowestOffer of
    Nothing ->
      return ()
    Just lowestOffer' -> do
      bestOffer' <- readTVar $ view bestOffer exch
      when (lowestOffer' < bestOffer') $
        writeTVar (view bestOffer exch) lowestOffer'
