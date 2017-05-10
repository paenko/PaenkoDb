source "./var.sh"

document_id=`RUST_LOG=raft=$log_level $binary post $lid $ip $file $username $password`

RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password
RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password
RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password
RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password
RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password
RUST_LOG=raft=$log_level $binary put $document_id $lid $ip $file $username $password

