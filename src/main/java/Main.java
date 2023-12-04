import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;
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
import java.util.stream.Collector;

public class Main {
    private static final String MYSQL_SCHEMA = "mysql";

    private static List<String> queryList = new ArrayList<>();
    private static List<SqlNode> sqlNodeList = new ArrayList<>();

    public static void main(String[] args) throws SQLException, SqlParseException, ValidationException {
        // JDBC 연결 설정
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Define our parser Configuration
        SqlParser.Config parserConfig = SqlParser.config();
        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // MySQL 객체 생성
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://localhost:3306/testdb?serverTimezone=UTC&characterEncoding=UTF-8",
                "com.mysql.jdbc.Driver",
                System.getenv("DB_USER"), // username
                System.getenv("DB_PASSWORD") // password
        );

        rootSchema.add(MYSQL_SCHEMA, JdbcSchema.create(rootSchema, MYSQL_SCHEMA, mysqlDataSource, null, null));

        Frameworks.ConfigBuilder config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));

        Planner planner = Frameworks.getPlanner(config.build());
        Scanner sc = new Scanner(System.in);
        String sql = sc.nextLine();
        System.out.println("sql = " + sql);
        queryList.add(sql);
        while(true){
        sql = sc.nextLine();
        System.out.println("sql = " + sql);
        if(!sql.equals("q"))    queryList.add(sql);
        else    break;
        }

        SqlNode node;
        SqlNode validateNode;
        SqlWriter writer;
        for(String query : queryList) {
            // Parsing
            node=planner.parse(query);

            // Validation
            validateNode = planner.validate(node);
            sqlNodeList.add(validateNode);

            SqlNodeVisitor visitor = new SqlNodeVisitor();
            validateNode.accept(visitor);

//            writer = new SqlPrettyWriter();
//            validateNode.unparse(writer, 0,0);
//            System.out.println(ImmutableList.of(writer.toSqlString().getSql()));
            planner.close();
        }

        // Print out our formatted SQL to the console
    }
    static class SqlNodeVisitor extends SqlShuttle {
        @Override
        public SqlNode visit(SqlCall node) {
            // Implement your logic here
            System.out.println("Visiting node: " + node.getClass().getSimpleName());
            return super.visit(node);
        }
    }
}

//    select * from "mysql"."payment"
//    select * from "mysql"."overdue"
//    select * from "mysql"."payment" JOIN "mysql"."user" ON "payment"."user_id" = "user"."id" WHERE "user"."id" = 1