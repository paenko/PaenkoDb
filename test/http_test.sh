source ./var.sh

normal=$'\e[0m'  
green=$(tput setaf 2) 
red="$bold$(tput setaf 1)" 
yellow="$bold$fawn"

curl --fail --verbose -X POST -c session_cookie -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "username":"'$username'",
	"password":"'$plain_password'"
	}' "$url/auth/login" && echo "$green login successful" || echo "$red failed login" 

echo "$normal"
sleep 1

doc_id=$(curl --fail --verbose -b session_cookie -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "payload":"dGVzVA==",
	 "version":1,
	"id":"wuarscht wird ersetzt"
}' "$url/document/$lid" ) && echo "$green document post successful" || echo "$red failed to  post data"

echo "$normal"
sleep 2 

curl --fail --verbose -b session_cookie -X GET "$url/document/$lid/$doc_id" && echo "$green document get successful" || echo "$red failed to get document"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X PUT -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
	"payload":"dXBkYXRlZA=="
}' "$url/document/$lid/document/$doc_id" && echo "$green updating document successful" || echo "$red updating document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X DELETE "$url/document/$lid/$doc_id" && echo "$green deleting document successful" || echo "$red deleting document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X GET "$url/meta/log/$lid/documents" && echo "$green fetching all documents was successful" || echo "$red failed to fetch all documents"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X GET "$url/meta/$lid/state/leader" && echo "$green get leader meta data"  || echo "$red failed to get leader meta data"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X GET "$url/meta/$lid/state/candidate" && echo "$green get candidate meta data" || echo "$red failed to get candidate meta data"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X GET "$url/meta/$lid/state/follower" && echo "$green get follower meta data" || echo "$red failed to get follower meta data"

echo "$normal"
sleep 1

echo "$yellow Rollback";
echo "$normal"

trans_id=$(curl --fail --verbose -b session_cookie -X POST "$url/transaction/begin/$lid") && echo "$green start transaction" || echo "$red failed to start transaction"

echo "$normal"
sleep 1

doc_id=$(curl --fail --verbose -b session_cookie -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "payload":"dGVzVA==",
	 "version":1,
	"id":"wuarscht wird ersetzt"
}' "$url/document/$lid/transaction/$trans_id"  ) && echo "$green document post successful" || echo "$red failed to post data"

echo "$normal"
sleep 2

curl --fail --verbose -b session_cookie -X PUT -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
	"payload":"dXBkYXRlZA=="
}' "$url/document/$lid/transaction/$trans_id/document/$doc_id" && echo "$green updating document successful" || echo "$red updating document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X DELETE "$url/document/$lid/$doc_id/transaction/$trans_id" && echo "$green deleting document successful" || echo "$red deleting document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X POST "$url/transaction/rollback/$lid/$trans_id" && echo "$green rollback successful" ||  echo "$red rollback failed"

sleep 1
echo "$yellow Commit";
echo "$normal"

trans_id=$(curl --fail --verbose -b session_cookie -X POST "$url/transaction/begin/$lid") && echo "$green start transaction" || echo "$red failed to start transaction"

echo "$normal"
sleep 1

doc_id=$(curl --fail --verbose -b session_cookie -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "payload":"dGVzVA==",
	 "version":1,
	"id":"wuarscht wird ersetzt"
}' "$url/document/$lid/transaction/$trans_id"  ) && echo "$green document post successful" || echo "$red failed to post data"

echo "$normal"
sleep 2

curl --fail --verbose -b session_cookie -X PUT -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
	"payload":"dXBkYXRlZA=="
}' "$url/document/$lid/transaction/$trans_id/document/$doc_id" && echo "$green updating document successful" || echo "$red updating document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X DELETE "$url/document/$lid/$doc_id/transaction/$trans_id" && echo "$green deleting document successful" || echo "$red deleting document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X POST "$url/transaction/commit/$lid/$trans_id" && echo "$green commit successful" ||  echo "$red commit failed"

