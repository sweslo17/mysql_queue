#!/usr/bin/expect

set timeout 1

set ip "10.0.1.5"

set user "rogerlo"

set password "roge2@iii"

spawn scp -r ./ "$user\@$ip:~/code/python/mysql_queue"

expect "Password:"

send "$password\r";

interact

set timeout 1

set ip "10.0.1.10"

set user "rogerlo"

set password "roge2@iii"

spawn scp -r ./ "$user\@$ip:~/code/python/mysql_queue"

expect "Password:"

send "$password\r";

interact
