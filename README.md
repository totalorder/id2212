# Installing
## Tools required
```
sudo apt-get install git maven
```
 
## Getting the code
```
git clone https://github.com/totalorder/id2212
```

## Compiling
```
mvn package
```

# Running

## Listen to port 5000, using schedule in schedule1.txt
```
java -cp target/u1-1.0-SNAPSHOT.jar org.deadlock.id2212.Main schedule1.txt 5000
```

## Listen to port 5001, connect to port 5000 on 127.0.0.1, using schedule in schedule2.txt
```
java -cp target/u1-1.0-SNAPSHOT.jar org.deadlock.id2212.Main schedule2.txt 5001 127.0.0.1 5000
```
## Listen to port 5002, connect to port 5000 on 127.0.0.1, using schedule in schedule3.txt
```
java -cp target/u1-1.0-SNAPSHOT.jar org.deadlock.id2212.Main schedule3.txt 5002 127.0.0.1 5000
```
