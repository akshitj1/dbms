import MySQLdb as sql
from warnings import filterwarnings, resetwarnings
import sys
from final_score import getFEntry 

conn = sql.connect(host="localhost", port=3306, user="root", passwd="", db="review_db")
csr = conn.cursor()
filterwarnings('ignore', category = conn.Warning)

prid='B0002L5R78'

qry="SET @pId=%s"
try:
	csr.execute(qry, (prid))
except sql.Error as e:
	print("error ", e)
	sys.exit(1)

qry="select reviewerId from rwRank where prodId = @pId limit 100"
try:
	csr.execute(qry)
except sql.Error as e:
	print("error ", e)
	sys.exit(1)

res = csr.fetchall()
for row in res:
	rid = row[0];
	print(rid)
	uVotes, dVotes, cScore, rwWritten, hScore, fScore, rwTxt = getFEntry(rid, prid, csr)
	qry="insert ignore into fTbl(reviewerId, prodId) values (@rId, @pId)"
	try:
		csr.execute(qry)
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)

	qry="""
	update fTbl
	set uVotes=%s, dVotes=%s, reviewText=%s, hScore=%s, cScore=%s, fScore=%s, noReviews=%s
	where reviewerId=@rId and prodId=@pId;"""
	try:
		csr.execute(qry, (uVotes, dVotes, rwTxt, hScore, cScore, fScore, rwWritten))
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)

	conn.commit()
	# break
