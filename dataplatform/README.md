# 安裝步驟
## Step 1
將檔案(.py與.sh檔)放置在路徑 ```/home/ecoetl```  
將api資料夾底下檔案複製到```/home/ecoetl/api```
如需換路徑，需要更改 ```crontab.process.sh``` 裡的檔案路徑。

## Step 2
在 ```/home/ecoetl``` 執行 
```
mkdir logs
```
## Step 3
安裝 Python 套件。如有安裝過此步驟可跳過。
```
# 系統層面
sudo apt install python3-pip
sudo apt install libmysqlclient-dev

# Python 層面
sudo pip3 install sqlalchemy==1.3.17
sudo pip3 install mysqlclient
sudo pip3 install pymysql
sudo pip3 install requests
sudo pip3 install pandas numpy
```

## Step 4
在 ```/home/ecoetl``` 執行 
```
bash crontab.process.sh
```
並進入 ```crontab -e``` 看是否更新成功。

## Step 5
執行mysql workbench, 複製sql資料夾底下的SQL檔並逐一貼到workbench執行


## Step 7
至DB看是否有資料進來，如果長時間沒有資料進來，則可檢查log。路徑：```/home/ecoetl/logs/```