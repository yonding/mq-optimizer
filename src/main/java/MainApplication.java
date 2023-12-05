import org.apache.calcite.adapter.jdbc.JdbcSchema;
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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.verdictdb.commons.DBTablePrinter;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MainApplication {
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
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));


//      3. Bundle multiple queries into a single batch *****************************************************************
        do {
            Planner planner = Frameworks.getPlanner(config.build());
            System.out.println("Please input a query.");
            Scanner sc = new Scanner(System.in);
            String sql = sc.nextLine();
            if (sql.equals("q")) {
                if(queryList.size()>1) {
                    System.out.println("----------------------------------------------\n");
                    break;
                }
                else{
                    System.out.println("You should input more than one query.");
                    System.out.println("----------------------------------------------\n");
                    continue;
                }
            }
            try {
                Query query = new Query(sql, planner);
                queryList.add(query);
                System.out.println("Query has been scheduled for batch processing.");
                System.out.println("ㄴ[Scheduled Query] : " + sql);
                System.out.println("----------------------------------------------\n");
            } catch (SqlParseException sqlParseException) {
                System.out.println("This query isn't parsable. Please check the query.");
                System.out.println("----------------------------------------------\n");
            } catch (ValidationException validationException) {
                System.out.println("This query is invalid. Please check the query.");
                System.out.println("----------------------------------------------\n");
            }
        } while (true);

        // print query list
        System.out.println("\n[Query List]");
        int number = 1;
        for (Query query : queryList) {
            System.out.println(number++ + ". " + query.sql);
        }
        System.out.println("----------------------------------------------\n");


//      4. Group queries into separate batches.
        List<List<Query>> batchList = MultiQueryOptimizer.separateQuery(queryList);


//      5. Generate Global RelNode
        RelBuilder relBuilder = RelBuilder.create(config.build());

        for(List<Query> queryList : batchList){
            RelNode node = MultiQueryOptimizer.makeGlobalQuery(relBuilder, queryList);
            relNodeList.add(node);
        }


//      6. Execute Optimized RelNode and Print ResultSet

        for(RelNode relNode : relNodeList){
            HepProgram program = HepProgram.builder().build();
            HepPlanner planner = new HepPlanner(program);
            planner.setRoot(relNode);
            RelNode optimizedNode = planner.findBestExp();

            final RelShuttle shuttle = new RelHomogeneousShuttle() {
                @Override
                public RelNode visit(TableScan scan) {
                    final RelOptTable table = scan.getTable();
                    if (scan instanceof LogicalTableScan && Bindables.BindableTableScan.canHandle(table)) {
                        return Bindables.BindableTableScan.create(scan.getCluster(), table);
                    }
                    return super.visit(scan);
                }
            };

            optimizedNode = optimizedNode.accept(shuttle);

            final RelRunner runner = connection.unwrap(RelRunner.class);
            PreparedStatement ps = runner.prepare(optimizedNode);

            ps.execute();

            ResultSet resultSet = ps.getResultSet();
            DBTablePrinter.printResultSet(resultSet);

        }

    }

}


//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON u."id" = p."user_id" WHERE p."method" = 'TOSSPAY'
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."user_id" = u."id" WHERE p."amount" >= 5000
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."discount_rate" = u."id" WHERE p."method" = 'TOSSPAY'
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."pid" = u."id" WHERE p."amount" >= 5000
//    select * from "mysql"."payment" AS p JOIN "mysql"."user" AS u ON p."user_id" = u."id" WHERE u."id" = 1
