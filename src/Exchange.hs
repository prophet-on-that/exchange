{-# LANGUAGE TemplateHaskell #-}

module Exchange where

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map
import qualified Focus
import Control.Concurrent.STM
import Control.Lens

type Price = Integer

data Quote = Quote
  { _clientId :: !Integer
  , _offerId :: !Integer
  , _count :: !Integer
  }

makeLenses ''Quote

data Quotes = Quotes
  { _bids :: ![Quote]
  , _offers :: ![Quote]
  } 

makeLenses ''Quotes

emptyQuotes :: Quotes
emptyQuotes
  = Quotes [] []

data Exchange = Exchange
  { _book :: Map Price Quotes
  , _bestBid :: TVar Price -- ^ The best bid is derived from the book, we store it here to avoid needless recomputation.
  , _bestOffer :: TVar Price -- ^ The best offer is derived from the book, we store it here to avoid needless recomputation.
  }

makeLenses ''Exchange

-- | Install a bid into the book. Bids will never exceed the best offer.
bid
  :: Quote
  -> Price -- ^ The maximum price the buyer is willing to meet. 
  -> Exchange
  -> STM ()
bid quote price exch 
  = Map.focus (Focus.alterM alter) price (view book exch)
  where
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter Nothing 
      = return . Just $ emptyQuotes
          & ( bids %~ (quote :) )
    alter (Just quotes)
      = return . Just $ quotes 
          & ( bids %~ (++ [quote]) )
  
-- | Install an offer into the book.
offer
  :: Quote
  -> Price
  -> Exchange
  -> STM ()
offer quote price exch 
  = Map.focus (Focus.alterM alter) price (view book exch)
  where
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter Nothing 
      = return . Just $ emptyQuotes
          & ( offers %~ (quote :) )
    alter (Just quotes)
      = return . Just $ quotes 
          & ( offers %~ (++ [quote]) )
  
