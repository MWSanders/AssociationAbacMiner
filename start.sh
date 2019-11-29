pkill -9 python
sudo service elasticsearch restart
sleep 20
curl -XDELETE localhost:9200/flat-*
#git reset
#git checkout .
#git clean -fdx
#git pull
python src/main.py
