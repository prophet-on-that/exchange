module Exchange where

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map
import qualified Focus
import Control.Concurrent.STM

type Price = Integer

data Offer = Offer
  { clientId :: !Integer
  , offerId :: !Integer
  , count :: !Integer
  }

data Offers = Offers
  { bids :: ![Offer]
  , asks :: ![Offer]
  } 

data Exchange = Exchange
  { book :: !(Map Price Offers)
  }

-- | Install a bid into the book.
bid
  :: Offer
  -> Price
  -> Exchange
  -> STM ()
bid offer price exch 
  = Map.focus (Focus.alterM alter) price (book exch)
  where
    alter :: Maybe Offers -> STM (Maybe Offers)
    alter Nothing 
      = return . Just $ Offers [offer] []
    alter (Just (Offers bids' asks'))
      = return . Just $ Offers (bids' ++ [offer]) asks'
  
  
  
