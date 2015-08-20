module TH_Utils
  ( unprefixedFields
  ) where

import Control.Lens
import Language.Haskell.TH
import Data.Maybe (maybeToList)
import Data.Char (toUpper)

unprefixedFields :: LensRules
unprefixedFields 
  = defaultFieldRules & lensField .~ namer
  where
    namer :: Name -> [Name] -> Name -> [DefName]
    namer _ _ field = maybeToList $ do
      return (MethodName (mkName cls) (mkName field'))
      where
        field' = nameBase field
        cls
          = (\(x:xs) -> "Has" ++ toUpper x : xs)  field'
