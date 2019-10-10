#!/bin/bash
# recursively check content of Local(NFS/SMB) and HDFS directories are THE SAME, file by file
# example: ./filechecker.bash /someLocalDir/srcDir/ /someHdfsDir/dstDir/

src=$1
dst=$2
dstFilelist=/tmp/dstFilelist.$$
srcFilelist=/tmp/srcFilelist.$$
RED='\033[0;31m' 
GREEN='\033[0;32m'
NC='\033[0m'
#Number of threads for md5 calculation
TN=3
log() {
	echo `date -u +"%Y-%b-%d %H:%M:%S UTC: "` $1
}
checkfiles() {
	firstLine=$1
	lastLine=$2
	numberOfLine=$(($lastLine - $firstLine + 1))
	echo "Checking $srcFilelist from line number $firstLine to $lastLine inclusive, $numberOfLine lines"
        for file in `tail -n +$firstLine $srcFilelist | head -n $numberOfLine | cut -d" " -f1`
        do
        	srcMd5=`md5sum $src$file | cut -d" " -f1`
        	dstMd5=`hadoop fs -cat $dst$file | md5sum | cut -d" " -f1`
        	if [[ "$srcMd5" = "$dstMd5" ]]
        	then
        		echo -e "Checked file $file and status ${GREEN}OK${NC}"
			echo `date -u +"%Y-%b-%d %H:%M:%S UTC: "` match md5 $dst$file >> successlog.txt
        	else
        		echo -e "Checked file $file and status ${RED}FAIL${NC}"
        		echo `date -u +"%Y-%b-%d %H:%M:%S UTC: "` unmatch md5 $dst$file >> faillog.txt
        	fi
        done
	echo "from $firstLine to $lastLine - DONE!"
}
( [ -z $src ] || [ -z $dst ] ) && log "[ERROR] Please give both src and dst path" && exit 1
log "[INFO] Creating $dstFilelist"
hadoop fs -ls -R $dst | grep -v "^d.*" | tr -s " " | awk -v DSTL=${#dst} '{print substr($8,DSTL+1),$5}' | sort -k 1 > $dstFilelist
log "[INFO] Creating $srcFilelist"
find $src -type f -exec ls -l {} + | awk -v SRCL=${#src} '{print substr($9,SRCL+1),$5}' | sort -k 1 > $srcFilelist
diff=`diff -q $srcFilelist $dstFilelist`
if [[ -z $diff ]]
then
	log "File list matches"
else
	log "[FAIL] File list does not match!"
	exit 1
fi
echo "Checking MD5 of all files from $dst with $src"
LN=`wc -l $srcFilelist | cut -d" " -f1`
log "Lines to check: $LN"
partSize=$(($LN / $TN))
log "All part size: $partSize"
lastPartSize=$(( $partSize + $LN%$TN ))
log "Last part size: $lastPartSize" 
i=0
while [[ i -lt $TN ]]
do
	if [[ i -lt $(($TN - 1)) ]]
	then
		checkfiles $(($i * $partSize + 1)) $(($i * $partSize + $partSize)) &
		((i++))
	else
		checkfiles $(($i * $partSize + 1)) $(($i * $partSize + $lastPartSize)) &
		((i++))
	fi
done
wait
