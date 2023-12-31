import  org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.verdictdb.commons.DBTablePrinter;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TestApplication {
    private static final String MYSQL_SCHEMA = "mysql";
    private static List<Query> queryList = new ArrayList<>();
    private static List<RelNode> relNodeList = new ArrayList<>();

    public static void main(String[] args) throws SQLException, SqlParseException, ValidationException, RelConversionException {

//      1. Make a CalciteConnection Instance ***************************************************************************
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SqlParser.Config parserConfig = SqlParser.config();
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://localhost:3306/testdb?serverTimezone=UTC&characterEncoding=UTF-8",
                "com.mysql.jdbc.Driver",
                System.getenv("DB_USER"), // username
                System.getenv("DB_PASSWORD") // password
        );


//      2. Add MySQL Data Source ***************************************************************************************
        rootSchema.add(MYSQL_SCHEMA, JdbcSchema.create(rootSchema, MYSQL_SCHEMA, mysqlDataSource, null, null));

        Frameworks.ConfigBuilder config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("mysql"))
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));


//      3. Bundle multiple queries into a single batch *****************************************************************
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"pid\" WHERE (\"point\" >= 99000 AND \"level\" < 3 AND \"method\" = 'PAYCO') OR (\"point\" > 99000 AND \"method\" = 'PAYCO')", config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE (\"id\" >= 59950 AND \"amount\" >= 49000) OR (\"id\" <= 10 AND \"method\" = 'TOSSPAY') OR (\"id\" <= 10 AND \"method\" ='APPLEPAY')", config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE (\"id\" >= 59950 AND \"amount\" >= 49000) OR (\"id\" <= 10 AND \"method\" = 'TOSSPAY') OR (\"id\" <= 10 AND \"method\" ='APPLEPAY')", config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE (\"id\" >= 59950 AND \"amount\" >= 49000) OR (\"id\" <= 10 AND \"method\" = 'TOSSPAY') OR (\"id\" <= 10 AND \"method\" ='APPLEPAY')", config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'TOSSPAY'", config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));
        queryList.add(new Query("select * from \"mysql\".\"payment\" JOIN \"mysql\".\"user\" ON \"id\" = \"user_id\" WHERE \"id\" <= 10 AND \"method\" = 'APPLEPAY'" , config));

        long startTime = System.nanoTime();

        // print query list
        System.out.println("\n-----------------[Query List]-----------------");
        int number = 1;
        for (Query q : queryList) {
            System.out.println(number++ + ". " + q.sql);
        }
        System.out.println("\n");


//      4. Group queries into separate batches.
        List<List<Query>> batchList = MultiQueryOptimizer.separateQuery(queryList);
        int index = 1;
        System.out.println("-----------------[BATCH LIST]-----------------");
        for(List<Query> batch : batchList){
            System.out.println("[BATCH "+index+++"]");
            for(Query q : batch){
                System.out.println(q.sql);
            }
            System.out.println("");
        }

//      5. Generate Global RelNode
        RelBuilder relBuilder = RelBuilder.create(config.build());

        for(List<Query> queryList : batchList){
            RelNode node = MultiQueryOptimizer.makeGlobalQuery(relBuilder, queryList);
            relNodeList.add(node);
        }

        index = 1;
//      6. Execute Optimized RelNode and Print ResultSet
        System.out.println("\n-----------------[Result Sets for Batches]-----------------");
        for(RelNode relNode : relNodeList){
            HepProgram program = HepProgram.builder().build();
            HepPlanner planner1 = new HepPlanner(program);
            planner1.setRoot(relNode);
            RelNode optimizedNode = planner1.findBestExp();

            final RelRunner runner = connection.unwrap(RelRunner.class);
            PreparedStatement ps = runner.prepare(optimizedNode);

            ps.execute();

            ResultSet resultSet = ps.getResultSet();
            System.out.println("\n[Result Set for Batch "+index+++"]");
            DBTablePrinter.printResultSet(resultSet);
            ps.close();
        }

        long endTime = System.nanoTime();

        // 소요된 시간 계산 (나노초)
        long elapsedTimeInNano = endTime - startTime;

        // 소요된 시간을 밀리초로 변환
        long elapsedTimeInMilli = elapsedTimeInNano / 1000000;

        System.out.println("소요된 시간 (나노초): " + elapsedTimeInNano);
        System.out.println("소요된 시간 (밀리초): " + elapsedTimeInMilli);
    }
}


//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON u."id" = p."user_id" WHERE p."method" = 'TOSSPAY'
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."discount_rate" = u."id" WHERE p."method" = 'TOSSPAY'
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."pid" = u."id" WHERE p."amount" >= 5000
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."user_id" = u."id" WHERE p."amount" >= 5000
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."user_id" = u."id" WHERE u."id" = 1
