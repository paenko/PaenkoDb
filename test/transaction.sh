source "./var.sh"


transaction_id=`RUST_LOG=raft=$log_level $binary begintrans $lid $ip $username $password`

sleep 2
echo "**begin transaction**"

document_id=`RUST_LOG=raft=$log_level $binary transpost $lid $ip $file $username $password ${transaction_id}`

echo "**Post transaction $document_id **"
sleep 1

RUST_LOG=raft=$log_level $binary transput $lid $ip ${document_id} $file $username $password ${transaction_id}

echo  "**Put transaction $document_id **"

sleep 1
RUST_LOG=raft=$log_level $binary transput $lid $ip ${document_id} $file $username $password ${transaction_id}
echo  "**Put transaction $document_id **"
sleep 1

RUST_LOG=raft=$log_level $binary transput $lid $ip ${document_id} $file $username $password ${transaction_id}

sleep 1

RUST_LOG=raft=$log_level $binary transremove $lid $ip ${document_id} $username $password ${transaction_id}

echo  "**Remove transaction $document_id **"

sleep 1

RUST_LOG=raft=$log_level $binary rollback $lid $ip $username $password $transaction_id
echo  "** Rollback transaction **"
