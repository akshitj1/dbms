#import database
mysql -uroot reviews_db < metaElectronics2.sql

#rename table
rename table metaElectronics3 to metadata;

#query results into csv
mysql review_db -uroot < query.sql | sed 's/\t/,/g'  > out.csv
#robust way
INTO OUTFILE '/tmp/out.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';


#get highest reviewed products
select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100;

#get prod names and categories
select t2.title, t2.catagories
from (select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100) as t1, 
	metadata as t2
where t1.prodId=t2.asin;

#create indexes
create index rid_idx on reviews(reviewerId);
create index pid_idx on reviews(prodId);


select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100;

select t2.title, t2.asin
from (select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100) as t1, 
	metadata as t2
where t1.prodId=t2.asin limit 1;

#categories with count in which user has written reviews
select t6.catagories, count(*) as num_reviews
from(
select t4.prodId
from(
select t2.reviewerId,count(*) as num_reviews
from(
select reviewerId from reviews where prodId='B0002L5R78'
) as t1, reviews as t2
where t2.reviewerId=t1.reviewerId
group by t2.reviewerId
order by num_reviews desc
limit 1
)as t3,  reviews as t4
where t4.reviewerId=t3.reviewerId
) as t5, metadata as t6
where t5.prodId=t6.asin
group by t6.catagories
order by num_reviews desc;

#find rank of all users
#A1Z4GII5CHCDG7
SET @i=0, @uWt=1, @dWt=2, @uid='A1Z4GII5CHCDG7';
select t1.reviewerId,(@uWt*t1.uVotes-@dWt*t1.dVotes) as score , @i:=@i+1 as rank
from (select reviewerId, phelp as uVotes, (ohelp-phelp) as dVotes from reviews where prodId='B0002L5R78') as t1
order by score desc limit 10;

SET @i=0, @pId='B0002L5R78';
SET @total=(select count(*) from rwRank where prodId=@pId);
drop temporary table if exists rTbl;
create temporary table if not exists rTbl
(select reviewerId,score , @i:=@i+1 as rank, @i/@total as relRank
from rwRank 
where prodId=@pId
order by score desc);

update rwRank t1, rTbl t2
set t1.rank=t2.relRank
where t1.prodId=@pId and t1.reviewerId=t2.reviewerId;

#rank of single user
SET @uWt=1, @dWt=2, @uid='A1Z4GII5CHCDG7';
select count(*) as rank, t4.total, (count(*)/t4.total) as relRank
from 
(select t1.reviewerId,(@uWt*t1.uVotes-@dWt*t1.dVotes) as score
from (select reviewerId, phelp as uVotes, (ohelp-phelp) as dVotes from reviews where prodId='B0002L5R78' and reviewerId=@uid) as t1) as t2,
(select t1.reviewerId,(@uWt*t1.uVotes-@dWt*t1.dVotes) as score
from (select reviewerId, phelp as uVotes, (ohelp-phelp) as dVotes from reviews where prodId='B0002L5R78') as t1) as t3,
(select count(*) as total from reviews where prodId='B0002L5R78') as t4
where t3.score>t2.score
;

#rank of single user. multiple temp tables as cant be used multiple times in single query
drop temporary table if exists sTbl;
create temporary table if not exists sTbl
(select reviewerId,(@uWt*(phelp)-@dWt*(ohelp-phelp)) as score from reviews where prodId='B0002L5R78' order by score desc);

drop temporary table if exists uTbl;
create temporary table if not exists uTbl
(select * from sTbl where reviewerId=@uid);

select count(*) as rank
from uTbl, sTbl
where sTbl.score>uTbl.score;

select count(*) as total
from sTbl;

#review rank table
drop table if exists rwRank;
create table rwRank
(
reviewerId varchar(255) DEFAULT NULL, 
prodId varchar(255) DEFAULT NULL,
score INT(11) DEFAULT 0, 
rank FLOAT(7,5) DEFAULT 0,
PRIMARY KEY (reviewerId, prodId),
FOREIGN KEY (reviewerId) REFERENCES reviews(reviewerId)
);
create index score_idx on rwRank(score);

#fill table
set @uWt=1, @dWt=2;
insert ignore into rwRank(reviewerId, prodId, score)
(
select t1.reviewerId, t1.prodId,(@uWt*(t1.phelp)-@dWt*(t1.ohelp-t1.phelp)) as score 
from reviews as t1,
(select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100) as t2
where t1.prodId=t2.prodId
);

#check if query assigned correct ranks
select * from rwRank where prodId='B0002L5R78' order by rank asc limit 10;

#get categories of reviewer
SET @rId='A6FIAB28IS79 ';
select t1.prodId, t1.rank, t2.catagories
from (select prodId, rank from rwRank where reviewerId=@rId) as t1,
metadata as t2
where t2.asin=t1.prodId;

#get counts of reviews written
select reviewerId, count(*) as cnt
from rwRank
group by reviewerId
order by cnt desc
limit 10;

SET @rId='A6FIAB28IS79', @pId='B000E6G9RI';
select ohelp, phelp, unixReviewTime
from reviews
where reviewerId=@rId and prodId=@pId;

DROP TABLE IF EXISTS `fTbl`;
CREATE TABLE `fTbl` (
  `reviewerId` varchar(255) DEFAULT NULL,
  `prodId` varchar(255) DEFAULT NULL,
  `uVotes` int(11) DEFAULT 0,
  `dVotes` int(11) DEFAULT 0,
  `reviewText` text,
  `hScore` FLOAT(8,5) DEFAULT 0,
  `cScore` FLOAT(8,5) DEFAULT 0,
  `fScore` FLOAT(8,5) DEFAULT 0,
  `noReviews` int(11) DEFAULT 0,
  PRIMARY KEY (reviewerId, prodId)
);
create index fScore_idx on fTbl(fscore);

SET @pId = 'B0002L5R78';
select reviewerId from rwRank where prodId = @pId limit 10;

set @rId='A6FIAB28IS79', @pId = 'B0002L5R78';
insert ignore into fTbl(reviewerId, prodId) values (@rId, @pId);

update fTbl
set uVotes=%s, dVotes=%s, reviewText=%s, hScore=%s, cScore=%s, fScore=%s, noReviews=%s
where reviewerId=%s, prodId=%s;