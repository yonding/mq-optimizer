# mq-optimizer

Multi-Query Optimizer Developed Using Apache Calcite (2023) <br><br>

##  Motivation
오늘날 질의 처리에 대한 기업들의 요구 수준이 높아짐에 따라 SharedDB, PsiDB와 같이 다중 질의 최적화를 지원하는 Concurrent DBMS(CDBMS) 연구가 이뤄지고 있다. 해당 연구 주제와 관련하여 다양한 연구를 시도하고 싶었으나 (내가 알기로는) 아직 오픈 소스로 공개된 다중 질의 최적기가 없어 연구를 진행할 수가 없었다. 공개된 Binary program도 존재하지 않았다. <br><br>
## Contribution
MQO, CDBMS와 관련된 다양한 연구를 진행하기 위해서 Apache Calcite를 사용하여 Multi-Query Optimization(MQO)를 지원하는 질의 처리기를 개발하게 되었다. MQO와 관련된 연구를 진행하고자 하는 연구자분들은 해당 코드를 수정하여 더욱 좋은 성능의 MQO를 개발하거나 CDBMS가 필요한 연구에 사용할 수 있다.  
<br><br>
## How to try?
1. 터미널에서 다음 명령어를 실행한다.
```bsh
git clone git@github.com:yonding/mq-optimizer.git
```
<br>

2. MultipleTestApplication 클래스의 다음 코드를 연결하고자 하는 데이터베이스에 맞게 설정한다.
```java
DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://localhost:3306/testdb?serverTimezone=UTC&characterEncoding=UTF-8",
                "com.mysql.jdbc.Driver",
                System.getenv("DB_USER"), // username
                System.getenv("DB_PASSWORD") // password
        );
```
<br>

3. MultipleTestApplication에서 처리하고자 하는 질의들을 지정한다. 다음은 예시 코드이다. 
```java
queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"point\" >= 99000 AND \"name\" = 'Willa' AND \"method\" = 'PAYCO'", config));
queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"point\" >= 99000 AND \"name\" = 'Willa' AND \"method\" = 'LPAY'", config));
queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 3 AND \"method\" = 'TOSSPAY'", config));
queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 3 AND \"method\" = 'TOSSPAY'", config));
```
<br>

4. MultipleTestApplication을 실행한다. 
