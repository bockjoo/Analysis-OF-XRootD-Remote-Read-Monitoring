#!/bin/bash
host_conda_base="/home/bockjoo/opt/cmsio2"
export X509_USER_PROXY=$HOME/my_voms.proxy
export PATH="/home/bockjoo/opt/cmsio2/anaconda3/bin:$PATH"

notifytowhom=y__empty__o__upseum__c__empty__k__upseum__j__empty__o__upseum__o__AT__gmail__dot__com
workdir=$HOME/opt/cmsio2/cms/services/T2/ops/Work/AAA
cd $workdir
i=0
#python aaaXRootDRemoteReadMonitoring.py
echo INFO creating $(pwd)/aaaXRootDRemoteReadMonitoring.json
python aaaXRootDRemoteReadMonitoring.py > aaaXRootDRemoteReadMonitoring.json
xrdremoteread=$(cat aaaXRootDRemoteReadMonitoring.json | sed "s#'_index'#\n'_index'#g" | grep -i "failed\|error")
#n80xx=$(printf "$xrdremoteread\n" | grep "'ExitCode': 80" | wc -l)
n80xx=$(printf "$xrdremoteread\n" | wc -l)
#echo INFO n80xx=$n80xx
printf "$xrdremoteread\n" | while read line ; do
    #echo $line | grep -q "'ExitCode': 80" || continue
    i=$(expr $i + 1)
    CRAB_JobLogURL=$(echo $line | sed "s#'CRAB_JobLogURL'#\n'CRAB_JobLogURL'#" | grep 'CRAB_JobLogURL' | cut -d\' -f4)
    [ "x$(echo $CRAB_JobLogURL)" == "x" ] && continue
    ExitCode=$(echo $line | sed "s#'ExitCode'#\n'ExitCode'#" | grep 'ExitCode' | cut -d\' -f3 | cut -d: -f2 | cut -d, -f1)
    #echo INFO ExitCode $ExitCode CRAB_JobLogURL $CRAB_JobLogURL
    #job_out=job_out.txt
    job_out=$(curl --capath /etc/grid-security/certificates --cacert $X509_USER_PROXY --cert $X509_USER_PROXY --key $X509_USER_PROXY -X GET $CRAB_JobLogURL 2>/dev/null | sed 's#%#%%#g') # > $job_out
    
    #ls -al $job_out
    
    job_wn=$(printf "$job_out\n" | grep "Current hostname" | cut -d: -f2)
    pfn=$(printf "$job_out\n" | grep "XrdCl::File::Open" | cut -d\' -f2)
    error_or_fail=$(printf "$job_out\n" | grep -i "fail\|error")
    exception_section=$(printf "$job_out\n" | grep -A 10000 "Begin Fatal Exception" | grep -B 10000 "End Fatal Exception")
    if [ "x$(echo $pfn)" == "x" ] ; then
	pfn=$(printf "$error_or_fail\n" | grep root://)
    fi
    if [ "x$(echo $pfn)" == "x" ] ; then
	pfn=$(printf "$exception_section\n" | grep root://)
    fi
    if [ "x$(echo $pfn)" == "x" ] ; then
	pfn=$(printf "$job_out\n" | grep "Initiating request to open file" | awk '{print $NF}')
    fi
    if [ "x$ExitCode" == "x8009" ] ; then
	files_failed_to_read="exception of category 'Configuration'"
    fi
    if [ "x$ExitCode" == "x8021" ] ; then
	files_failed_to_read=$(printf "$exception_section\n" | grep "Can" | grep "read input file" | awk '{print $NF}' | sort -u)
    fi
    if [ "x$ExitCode" == "x8022" ] ; then
	files_failed_to_read="Fatal ROOT ERROR"
    fi
    if [ "x$ExitCode" == "x8028" ] ; then
	files_failed_to_read=$(printf "$exception_section\n" | grep '\[ERROR\]')
    fi
    if [ "x$ExitCode" == "x8901" ] ; then
	files_failed_to_read="Segmentation fault"
    fi
    if [ "x$ExitCode" == "x50660" ] ; then
	files_failed_to_read="==== Failed to load the long exit code from jobReport.exitCode.txt. Falling back to short exit code ====
======== Short exit code also missing. Settint exit code to 80001 ========
"
    fi
    if [ "x$ExitCode" == "x50115" ] ; then
	files_failed_to_read="Execution site for failed job from site-local-config.xml"
    fi
    pfn_socket_error=
    printf "$job_out\n" | grep "Socket error while handshaking" | grep -q "Socket timeout"
    if [ $? -eq 0 ] ; then
	pfn_socket_error=$(printf "$job_out\n" | grep -B 1 "Socket error while handshaking" | grep -B 1 "Socket timeout" | grep root | awk '{print $NF}')
    fi
    no_servers=
    printf "$job_out\n" | grep -q 'No additional data servers were found\|Server responded with an error: \[3011\] No servers have the file'
    if [ $? -eq 0 ] ; then
	no_servers=$(printf "$job_out\n" | grep 'No servers are available to read the file' | cut -d\' -f2)
    fi

    [ $(echo "$pfn" | grep -q /store ; echo $?) -eq 0 ] || continue
    
    echo $pfn | grep -q "belforte/GenericTTbar/Stefano" && continue
    lfn=$(echo $pfn | sed "s#/store# /store#" | awk '{print $2}')
    sites=$($HOME/bin/sdas --ruciolfnse=$lfn | grep -i -v tape | grep -i -v buffer)
    description_of_exception=$(printf "$exception_section\n" | grep "An exception of category" | cut -d\' -f2)
    if [ "x$(echo $description_of_exception)" == "x" ] ; then
      description_of_exception=$(printf "$job_out\n" | grep "An exception of category" | cut -d\' -f2)
    fi
    #echo "[ $i / $n80xx ]" $job_wn "|" $ExitCode "|" $sites "|" $pfn "|" $CRAB_JobLogURL "|" $files_failed_to_read
    echo "[ $i / $n80xx ]" $job_wn "|" $ExitCode Exception=$description_of_exception "|" $sites "|" $pfn "|" $CRAB_JobLogURL "|" $files_failed_to_read "|"
    if [ "x$no_servers" != "x" ] ; then
       echo "[ $i / $n80xx ] [XRootD] No servers are available to read the file=$no_servers"
    fi
    if [ "x$pfn_socket_error" != "x" ] ; then
       echo "[ $i / $n80xx ] [XRootD] Socket_timeout_pfn=$pfn_socket_error"
    fi
    echo xrdfs result:
    output=$(xrdfs  cms-xrd-global.cern.ch:1094 locate -m -d $lfn)
    printf "$output\n" | grep -q "No servers have the file"
    status=$?
    if [ $status -eq 0 ] ; then
	echo No servers have the file
    else
	printf "$output\n"
    fi
    
    echo Job XRootD Error:
    printf "$error_or_fail\n"
    echo Job Exception:
    printf "$exception_section\n"
    echo ; echo ; echo
done
(
     echo "To: "$(echo $notifytowhom | sed "s#__AT__#@#" | sed "s#__dot__#\.#g" | sed "s#__empty__##g" | sed "s#__upseum__##g")
     #echo "Cc: "$(echo $notifytowhom | sed "s#__AT__#@#" | sed "s#__dot__#\.#g" | sed "s#__empty__##g" | sed "s#__upseum__##g")
     echo "Subject: aaaAnalyzeXRootDRemoteReadMonitoring on $(/bin/hostname -s)"
     echo "Content-Type: text/html"
     #echo "<html>"
     echo "<pre>"
     cat $HOME/opt/cmsio2/cms/services/T2/ops/Work/AAA/aaaAnalyzeXRootDRemoteReadMonitoring.log
     echo "</pre>"
     #echo "</html>"
) | /usr/sbin/sendmail -t

cd -

exit 0
