import MySQLdb as sql
from warnings import filterwarnings, resetwarnings
import sys
import time
import math

def getDepth(c1,c2):
	c1 = c1.replace('[','');
	c1 = c1.replace(']','');
	c2 = c2.replace('[','');
	c2 = c2.replace(']','');

	l1 = c1.split(',')
	l2 = c2.split(',')

	d=0
	while d<min(len(l1),len(l2)):
		if(l1[d]!=l2[d]):
			#print(d)
			break;
		d = d+1;
	return d;

def getScore(rank, depth):
	return depth/(rank**0.5)

def getHscore(rid, prid, csr, deb=False):
	qry="SET @rId=%s, @pId=%s"
	try:
		csr.execute(qry, (rid, prid))
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)

	qry="select catagories from metadata where asin=@pId";
	try:
		csr.execute(qry)
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)
	res=csr.fetchall()
	res=res[0]
	curCat=res[0]
	if deb: print("current category is: ", curCat)

	qry="""select t1.prodId, t1.rank, t2.catagories
	from (select prodId, rank from rwRank where reviewerId=@rId) as t1,
	metadata as t2
	where t2.asin=t1.prodId;"""
	try:
		csr.execute(qry)
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)
	res=csr.fetchall()
	hScore=0
	for row in res:
		if deb: print(row)
		cati=row[2]
		depth=getDepth(curCat, cati)
		rank=row[1]
		score=getScore(rank, depth)
		hScore+=score
	return hScore, len(res)

def getCScore(rid, prid, csr, deb=False):
	

	qry="""select ohelp, phelp, unixReviewTime, reviewText
	from reviews
	where reviewerId=@rId and prodId=@pId limit 1;"""
	try:
		csr.execute(qry)
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)
	res=csr.fetchall()
	res=res[0]
	uVotes=res[1]
	dVotes=res[0]-res[1]
	rwTxt=res[3]
	tElps=int(time.time())-res[2]
	if deb: print(uVotes, dVotes, tElps)
	uWt=0.33
	dWt=-(1-uWt)
	return uVotes, dVotes, (uVotes*uWt + dWt*dVotes)/math.log(1+tElps), rwTxt#+1 to avoid division by zero

def getFEntry(rid, prid, csr, deb=False):
	# conn = sql.connect(host="localhost", port=3306, user="root", passwd="", db="review_db")
	# csr = conn.cursor()
	# filterwarnings('ignore', category = conn.Warning)
	qry="SET @rId=%s, @pId=%s"
	try:
		csr.execute(qry, (rid, prid))
	except sql.Error as e:
		print("error ", e)
		sys.exit(1)
	# rid='A6FIAB28IS79'
	# prid='B000E6G9RI'
	hScore, rwWritten=getHscore(rid, prid, csr)
	uVotes, dVotes, cScore, rwTxt=getCScore(rid, prid, csr)
	
	if deb: print("history Score: ", hScore, "current Score: ", cScore)
	hWt=1/50.0
	cWt=1-hWt
	initScore=0.5
	hScore=hWt*hScore
	cScore=cWt*cScore
	fScore=hScore+cScore+initScore
	if deb: print("weighted history Score: ", hScore, "weighted current Score: ", cScore)
	if deb: print("final score: ", fScore)
	if deb: print(uVotes, dVotes, cScore, rwWritten, hScore, fScore)
	return (uVotes, dVotes, cScore, rwWritten, hScore, fScore, rwTxt)


if __name__=="__main__":
	rid='A6FIAB28IS79'
	prid='B000E6G9RI'
	conn = sql.connect(host="localhost", port=3306, user="root", passwd="", db="review_db")
	csr = conn.cursor()
	updateEntry(rid, prid, csr)
