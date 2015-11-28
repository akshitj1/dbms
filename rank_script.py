import MySQLdb

db = MySQLdb.connect(host="localhost", port=3306, user="root", passwd="", db="review_db")
csr = db.cursor()
qry="select prodId, count(*) as review_cnt from reviews group by prodId order by review_cnt DESC  limit 100"
csr.execute(qry)
res=csr.fetchall()
for row in res:
	pid=row[0]
	print(pid)
	qry="SET @i=0, @pId='%s';"
	csr.execute(qry,(pid))

	qry="SET @total=(select count(*) from rwRank where prodId=@pId);"
	qry="drop temporary table if exists rTbl;"
	qry="""create temporary table if not exists rTbl
	(select reviewerId,score , @i:=@i+1 as rank, @i/@total as relRank
	from rwRank 
	where prodId=@pId
	order by score desc);"""

	qry="""update rwRank t1, rTbl t2
	set t1.rank=t2.relRank
	where t1.prodId=@pId and t1.reviewerId=t2.reviewerId;"""
	break