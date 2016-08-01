{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module ExchangeSim.Book where

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map
import qualified Focus
import Control.Concurrent.STM (STM, TVar)
import qualified Control.Concurrent.STM as STM
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

data Quote = Quote
  { clientRef :: !Id
  , quoteId :: !Id
  , quantity :: !Integer
  , price :: !Price -- ^ Limiting price on the quote.
  , timestamp :: {-# UNPACK #-} !UTCTime
  } deriving (Eq, Show)

instance Ord Quote where
  compare
    = comparing quoteId

type Bid = Quote
type Offer = Quote

data Quotes = Quotes
  { bids :: !(Set Quote)
  , offers :: !(Set Quote)
  } 
 
emptyQuotes :: Quotes
emptyQuotes
  = Quotes Set.empty Set.empty

-- | Pre: bestBid <= bestOffer.
data Book = Book
  { book :: Map Price Quotes
  , bestBid :: TVar (Maybe Price)
  , bestOffer :: TVar (Maybe Price)
  }

newBook :: STM Book
newBook
  = Book
      <$> Map.new
      <*> STM.newTVar Nothing
      <*> STM.newTVar Nothing

newtype BookOp a = BookOp
  { exchangeOp :: ReaderT Book STM a
  } deriving (Functor, Applicative, Monad, MonadReader Book, MonadBase STM, MonadThrow, MonadCatch)

data Transaction = Transaction
  { buyerRef :: Id
  , sellerRef :: Id
  , transactionQty :: Integer
  , transactionPrice :: Price
  } deriving (Show)

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
      Book {..} <- ask
      bestBid' <- readTVar bestBid
      bestOffer' <- readTVar bestOffer
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
          quotes <- liftBase $ Map.focus lookupAndDelete tradePrice' book
          case quotes of
            Nothing ->
              throwM $ InconsistentState "resolve: book lookup at tradePrice yielded Nothing"
            Just Quotes {..} -> do
              let
                bids'
                  = sortBy (comparing timestamp) $ Set.toList bids
                offers'
                  = sortBy (comparing timestamp) $ Set.toList offers
                (transactions, remainingBids, remainingOffers)
                  = resolve' tradePrice' bids' offers'

              -- Recompute best bid.
              when (null remainingBids) $ do
                let
                  helper price' (price, Quotes {..})
                    | Set.size bids > 0
                        = case price' of
                            Nothing ->
                              return $ Just price
                            Just price'' ->
                              return . Just $ max price'' price
                    | otherwise
                        = return price'
                newBestBid <- liftBase . ListT.fold helper Nothing . Map.stream $ book
                writeTVar bestBid newBestBid

              -- Recompute best offer.
              when (null remainingOffers) $ do
                let
                  helper price' (price, Quotes {..})
                    | Set.size offers > 0
                        = case price' of
                            Nothing ->
                              return $ Just price
                            Just price'' ->
                              return . Just $ min price'' price
                    | otherwise
                        = return price'
                newBestOffer <- liftBase . ListT.fold helper Nothing . Map.stream $ book
                writeTVar bestOffer newBestOffer
                
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
        resolve'
          :: Price -> [Bid] -> [Offer] -> ([Transaction], [Bid], [Offer])
        resolve' tradePrice
          = helper []
          where
            helper :: [Transaction] -> [Bid] -> [Offer] -> ([Transaction], [Bid], [Offer])
            helper transactions (bid : bids') (offer : offers')
              | bidQuantity == offerQuantity
                  = let
                      newTransaction
                        = Transaction buyerRef sellerRef bidQuantity tradePrice
                    in
                      helper (newTransaction : transactions) bids' offers'
              | bidQuantity > offerQuantity
                  = let
                      newTransaction
                        = Transaction buyerRef sellerRef offerQuantity tradePrice
                      updatedBid
                        = bid { quantity = bidQuantity - offerQuantity}
                    in
                      helper (newTransaction : transactions) (updatedBid : bids') offers'
              | otherwise 
                  = let
                      newTransaction
                        = Transaction buyerRef sellerRef bidQuantity tradePrice
                      updatedOffer
                        = offer { quantity = offerQuantity - bidQuantity }
                    in
                      helper (newTransaction : transactions) bids' (updatedOffer : offers')
              where
                bidQuantity
                  = quantity bid
                offerQuantity
                  = quantity offer
                buyerRef
                  = clientRef bid
                sellerRef
                  = clientRef offer
    
            helper transactions bids' offers'
              = (transactions, bids', offers')
                
-- | Install a 'Quote' as a bid into the book. 
addBid
  :: Bid
  -> BookOp ()
addBid bid@Quote {..} = do
  Book {..} <- ask 
  bestOffer' <- readTVar bestOffer
  let
    bidPrice
      = case bestOffer' of
          Nothing ->
            price
          Just bestOffer'' ->
            min price bestOffer''
            
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter
      = return . Just . modQuotes . fromMaybe emptyQuotes
      where
        modQuotes quotes@Quotes {..}
          = quotes { bids = Set.insert bid bids }

  liftBase $ Map.focus (Focus.alterM alter) bidPrice book

  -- Update stored best bid.
  bestBid' <- readTVar bestBid
  case bestBid' of
    Nothing ->
      writeTVar bestBid (Just bidPrice)
    Just bestBid'' ->
      writeTVar bestBid (Just $ max bidPrice bestBid'')

-- | Install a 'Quote' as an offer into the book. 
addOffer
  :: Offer
  -> BookOp ()
addOffer offer@Quote {..} = do
  Book {..} <- ask 
  bestBid' <- readTVar bestBid
  let 
    offerPrice
      = case bestBid' of
          Nothing ->
            price
          Just bestBid'' -> 
            max price bestBid''
        
    alter :: Maybe Quotes -> STM (Maybe Quotes)
    alter 
      = return . Just . modQuotes . fromMaybe emptyQuotes
      where
        modQuotes quotes@Quotes {..}
          = quotes { offers = Set.insert offer offers }

  liftBase $ Map.focus (Focus.alterM alter) offerPrice book

  -- Update stored best offer.
  bestOffer' <- readTVar bestOffer
  case bestOffer' of
    Nothing ->
      writeTVar bestOffer (Just offerPrice)
    Just bestOffer'' -> 
      writeTVar bestOffer (Just $ min offerPrice bestOffer'')

