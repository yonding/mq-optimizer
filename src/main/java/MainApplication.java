import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MainApplication {
    private static final String MYSQL_SCHEMA = "mysql";
    private static List<String> queryList = new ArrayList<>();
    private static List<SqlNode> sqlNodeList = new ArrayList<>();

    public static void main(String[] args) throws SQLException, SqlParseException, ValidationException {

//      1. Make a CalciteConnection Instance **************************************************
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


//      2. Add MySQL Data Source **************************************************************
        rootSchema.add(MYSQL_SCHEMA, JdbcSchema.create(rootSchema, MYSQL_SCHEMA, mysqlDataSource, null, null));

        Frameworks.ConfigBuilder config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));


//      3. Bundle multiple queries into a single batch ****************************************
        do {
            Planner planner = Frameworks.getPlanner(config.build());
            Scanner sc = new Scanner(System.in);
            String sql = sc.nextLine();
            if(sql.equals("q")) break;
            try{
                // Validation
                SqlNode node = planner.parse(sql);
                planner.validate(node);
            }catch(SqlParseException sqlParseException){
                System.out.println("This query isn't parsable. Please check the query.");
                continue;
            }
            catch (ValidationException validationException){
                System.out.println("This query is invalid. Please check the query.");
                continue;
            }
            queryList.add(sql);
            System.out.println("Query has been scheduled for batch processing.");
            System.out.println("ㄴ[Scheduled Query] : " + sql);
        }while(true);

        for(String query : queryList) {
            System.out.println("query = " + query);
        }
    }


}


//    select * from "mysql"."payment"
//    select * from "mysql"."overdue"
//    select * from "mysql"."payment" JOIN "mysql"."user" ON "payment"."user_id" = "user"."id" WHERE "user"."id" = 1
