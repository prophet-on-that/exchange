module ExchangeSim.Exchange where

import ExchangeSim.Book

import STMContainers.Set (Set)
import qualified STMContainers.Set as Set
import qualified Data.Text as T
import Control.Concurrent.STM

data Exchange = Exchange
  { listings :: Set Listing
  }

newExchange :: STM Exchange
newExchange
  = Exchange <$> Set.new

type ListingId = Integer

data Listing = Listing
  { listingId :: ListingId
  , listingName :: T.Text
  , book :: Book
  }

instance Eq Listing where
  l == l'
    = listingId l ==  listingId l'

instance Ord Listing where
  compare l l'
    = compare (listingId l) (listingId l')

newListing
  :: ListingId
  -> T.Text
  -> STM Listing
newListing id name 
  = Listing id name <$> newBook

