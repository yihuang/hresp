{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.RESP
  ( Reply(..), renderReply, parseReply
  , Request(..), renderRequest, parseRequest
  ) where

import Prelude hiding (error, take)
import Data.Maybe (fromMaybe)
import Control.Applicative
import Data.Attoparsec.ByteString (takeTill)
import Data.Attoparsec.ByteString.Char8 hiding (takeTill)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B

-- |Low-level representation of replies from the Redis server.
data Reply = SingleLine ByteString
           | Error ByteString
           | Integer Integer
           | Bulk (Maybe ByteString)
           | MultiBulk (Maybe [Reply])
         deriving (Eq, Show)

-- | Do we need to support Null in request?
newtype Request = Request [ByteString]

------------------------------------------------------------------------------
-- Request
--
renderRequest :: Request -> ByteString
renderRequest (Request args) =
    B.concat $ ["*", showBS (length args)] ++ concatMap renderBulk args

parseBulk :: Parser (Maybe ByteString)
parseBulk = do
    len <- char '$' *> signed decimal <* endOfLine
    if len < 0
        then return Nothing
        else Just <$> take len <* endOfLine

parseRequest :: Parser Request
parseRequest = Request <$> do
    len <- char '*' *> signed decimal <* endOfLine
    if len < 0
        then return []
        else count len (fromMaybe "" <$> parseBulk)

------------------------------------------------------------------------------
-- Reply parsers
--

showBS :: (Show a) => a -> ByteString
showBS = B.pack . show

crlf :: ByteString
crlf = "\r\n"

renderBulk :: ByteString -> [ByteString]
renderBulk bs = ["$", showBS (B.length bs), crlf, bs, crlf]

renderReply :: Reply -> ByteString
renderReply reply = B.concat (render reply)
  where
    render :: Reply -> [ByteString]
    render (SingleLine bs) = ["+", bs]
    render (Error bs) = ["-", bs]
    render (Integer n) = [":", showBS n]
    render (Bulk Nothing) = ["$-1"]
    render (Bulk (Just bs)) = renderBulk bs
    render (MultiBulk Nothing) = ["*-1"]
    render (MultiBulk (Just replies)) = ["*", showBS (length replies)] ++ concatMap render replies

parseReply :: Parser Reply
parseReply = choice [singleLine, integer, bulk, multiBulk, error]

singleLine :: Parser Reply
singleLine = SingleLine <$> (char '+' *> takeTill isEndOfLine <* endOfLine)

error :: Parser Reply
error = Error <$> (char '-' *> takeTill isEndOfLine <* endOfLine)

integer :: Parser Reply
integer = Integer <$> (char ':' *> signed decimal <* endOfLine)

bulk :: Parser Reply
bulk = Bulk <$> parseBulk

multiBulk :: Parser Reply
multiBulk = MultiBulk <$> do
    len <- char '*' *> signed decimal <* endOfLine
    if len < 0
        then return Nothing
        else Just <$> count len parseReply
