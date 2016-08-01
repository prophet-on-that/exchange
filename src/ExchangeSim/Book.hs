{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ViewPatterns #-}

module ExchangeSim.Book where

import ExchangeSim.TH_Utils

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map
import qualified Focus
import Control.Concurrent.STM (STM, TVar)
import qualified Control.Concurrent.STM as STM
import Control.Lens
import Data.Time
import Control.Monad
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Ord
import Data.List (sortBy)
import Control.Monad.Catch
import qualified Data.Text as T
import Data.Typeable
import Control.Monad.Reader
import Control.Monad.Base
import qualified ListT

-- STM primitives --

readTVar :: MonadBase STM m => TVar a -> m a
readTVar
  = liftBase . STM.readTVar

writeTVar :: MonadBase STM m => TVar a -> a -> m ()
writeTVar tVar a
  = liftBase $ STM.writeTVar tVar a

-- Implementation proper --

type Price = Integer

newtype Id = Id Integer
  deriving (Eq, Ord, Show)

declareLensesWith unprefixedFields [d|
  data Bid = Bid
    { clientRef :: !Id
    , bidId :: !Id
    , quantity :: !Integer
    , maximumPrice :: !Price
    , timestamp :: {-# UNPACK #-} !UTCTime
    } deriving (Eq, Show)
  |]

instance Ord Bid where
  compare
    = comparing (view bidId)

declareLensesWith unprefixedFields [d|
  data Offer = Offer
    { clientRef :: !Id
    , offerId :: !Id
    , quantity :: !Integer
    , minimumPrice :: !Price
    , timestamp :: {-# UNPACK #-} !UTCTime
    } deriving (Eq, Show)
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
  -- | Pre: bestBid <= bestOffer.
  data Book = Book
    { book :: Map Price Quotes
    , bestBid :: TVar (Maybe Price) -- ^ The best bid is derived from the book, we store it here to avoid needless recomputation.
    , bestOffer :: TVar (Maybe Price) -- ^ The best offer is derived from the book, we store it here to avoid needless recomputation.
    }
  |]

newBook :: STM Book
newBook
  = Book
      <$> Map.new
      <*> STM.newTVar Nothing
      <*> STM.newTVar Nothing

newtype BookOp a = BookOp
  { exchangeOp :: ReaderT Book STM a
  } deriving (Functor, Applicative, Monad, MonadReader Book, MonadBase STM, MonadThrow, MonadCatch)

declareLensesWith unprefixedFields [d|
  data Transaction = Transaction
    { buyerRef :: Id
    , sellerRef :: Id
    , quantity :: Integer
    , price :: Price
    } deriving (Show)
  |]

data BookException
  = InconsistentState T.Text
  deriving (Show, Typeable)

instance Exception BookException

runBookOp :: Book -> BookOp () -> STM [Transaction]
runBookOp exch op
  = runReaderT (exchangeOp $ op >> resolve) exch
  where
    resolve :: BookOp [Transaction]
    resolve = do
      bestBid' <- view bestBid >>= readTVar
      bestOffer' <- view bestOffer >>= readTVar
      let
        tradePrice = do
          bestBid'' <- bestBid'
          bestOffer'' <- bestOffer'
          guard $ bestBid'' == bestOffer''
          return bestBid''
            
      case tradePrice of
        Nothing ->
          return []
        Just tradePrice' -> do
          quotes <- view book >>= liftBase . Map.focus lookupAndDelete tradePrice'
          case quotes of
            Nothing ->
              throwM $ InconsistentState "resolve: book lookup at tradePrice yielded Nothing"
            Just quotes' -> do
              let
                bids'
                  = sortBy (comparing $ view timestamp) . Set.toList . view bids $ quotes'
                offers'
                  = sortBy (comparing $ view timestamp) . Set.toList . view offers $ quotes'
                (transactions, remainingBids, remainingOffers)
                  = resolve' tradePrice' bids' offers'

              -- Recompute best bid.
              when (null remainingBids) $ do
                let
                  helper price' (price, view bids -> bids')
                    | Set.size bids' > 0
                        = case price' of
                            Nothing ->
                              return $ Just price
                            Just price'' ->
                              return . Just $ max price'' price
                    | otherwise
                        = return price'
                bestBid' <- view book >>= liftBase . ListT.fold helper Nothing . Map.stream
                view bestBid >>= flip writeTVar bestBid'

              -- Recompute best offer.
              when (null remainingOffers) $ do
                let
                  helper price' (price, view offers -> offers')
                    | Set.size offers' > 0
                        = case price' of
                            Nothing ->
                              return $ Just price
                            Just price'' ->
                              return . Just $ min price'' price
                    | otherwise
                        = return price'
                bestOffer' <- view book >>= liftBase . ListT.fold helper Nothing . Map.stream
                view bestOffer >>= flip writeTVar bestOffer'
                
              case (remainingBids, remainingOffers) of
                ([], []) ->
                  return transactions
                  
                (remainingBids', []) -> do
                  mapM_ addBid remainingBids' 
                  transactions' <- resolve 
                  return $ transactions ++ transactions'
                  
                ([], remainingOffers') -> do
                  mapM_ addOffer remainingOffers' 
                  transactions' <- resolve 
                  return $ transactions ++ transactions'
    
                _ ->
                  throwM $ InconsistentState "resolve: bids and offers both non-exhausted"
      where
        lookupAndDelete :: Focus.StrategyM STM Quotes (Maybe Quotes)
        lookupAndDelete Nothing
          = return $ (Nothing, Focus.Keep)
        lookupAndDelete (Just quotes)
          = return $ (Just quotes, Focus.Remove)
        
        -- Post: null bids || null offers
        resolve' :: Price -> [Bid] -> [Offer] -> ([Transaction], [Bid], [Offer])
        resolve' tradePrice
          = helper []
          where
            helper :: [Transaction] -> [Bid] -> [Offer] -> ([Transaction], [Bid], [Offer])
            helper transactions (bid : bids') (offer : offers')
              | bidQuantity == offerQuantity
                  = let
                      newTransaction
                        = Transaction buyerRef' sellerRef' bidQuantity tradePrice
                    in
                      helper (newTransaction : transactions) bids' offers'
              | bidQuantity > offerQuantity
                  = let
                      newTransaction
                        = Transaction buyerRef' sellerRef' offerQuantity tradePrice
                      updatedBid
                        = bid
                            & ( quantity -~ offerQuantity )
                    in
                      helper (newTransaction : transactions) (updatedBid : bids') offers'
              | otherwise 
                  = let
                      newTransaction
                        = Transaction buyerRef' sellerRef' bidQuantity tradePrice
                      updatedOffer
                        = offer
                            & ( quantity -~ bidQuantity )
                    in
                      helper (newTransaction : transactions) bids' (updatedOffer : offers')
              where
                bidQuantity
                  = view quantity bid
                offerQuantity
                  = view quantity offer
                buyerRef'
                  = view clientRef bid
                sellerRef'
                  = view clientRef offer
    
            helper transactions bids' offers'
              = (transactions, bids', offers')
                
-- | Install a Bid into the book. 
addBid
  :: Bid
  -> BookOp ()
addBid bid = do
  bestOffer' <- view bestOffer >>= readTVar
  let
    bidPrice
      = case bestOffer' of
          Nothing ->
            view maximumPrice bid
          Just bestOffer'' ->
            min (view maximumPrice bid) bestOffer''
            
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter
      = return . Just . modQuotes . fromMaybe emptyQuotes
      where
        modQuotes
          = bids %~ Set.insert bid

  book' <- view book
  liftBase $ Map.focus (Focus.alterM alter) bidPrice book'

  -- Update stored best bid.
  bestBid' <- view bestBid >>= readTVar
  case bestBid' of
    Nothing ->
      view bestBid >>= flip writeTVar (Just bidPrice)
    Just bestBid'' ->
      view bestBid >>= flip writeTVar (Just $ max bidPrice bestBid'')

-- | Install an 'Offer' into the book. 
addOffer
  :: Offer
  -> BookOp ()
addOffer offer = do
  bestBid' <- view bestBid >>= readTVar
  let 
    offerPrice
      = case bestBid' of
          Nothing ->
            view minimumPrice offer
          Just bestBid'' -> 
            max (view minimumPrice offer) bestBid''
        
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter 
      = return . Just . modQuotes . fromMaybe emptyQuotes
      where
        modQuotes
          = offers %~ Set.insert offer

  book' <- view book
  liftBase $ Map.focus (Focus.alterM alter) offerPrice book'

  -- Update stored best offer.
  bestOffer' <- view bestOffer >>= readTVar
  case bestOffer' of
    Nothing ->
      view bestOffer >>= flip writeTVar (Just offerPrice)
    Just bestOffer'' -> 
      view bestOffer >>= flip writeTVar (Just $ min offerPrice bestOffer'')

