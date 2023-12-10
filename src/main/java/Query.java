import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.sql.ResultSet;
import java.util.*;

public class Query {
    String sql;
    SqlNode sqlNode;
    List nodeList;

    // 질의 정보
    List selectList;
    SqlJoin sqlJoin;

    Planner planner;
    List<String> joinConditionList;
    Frameworks.ConfigBuilder config;
    ResultSet resultSet;

    Query(String sql, Frameworks.ConfigBuilder config) throws SqlParseException, ValidationException, RelConversionException {
        this.planner = Frameworks.getPlanner(config.build());
        this.config = config;
        this.sql = sql;
        this.sqlNode = planner.validate(planner.parse(sql));
//        RelNode relNode = planner.rel(sqlNode).project();
        this.nodeList = ((SqlSelect) sqlNode).getOperandList();
        this.selectList = (SqlNodeList) this.nodeList.get(1);
        this.sqlJoin = (SqlJoin) nodeList.get(2);
        for(SqlNode whereInfo : ((SqlBasicCall) ((SqlSelect) sqlNode).getWhere()).getOperandList())
        this.joinConditionList = new ArrayList<>();
        for (SqlNode operand : ((SqlBasicCall)this.sqlJoin.getCondition()).getOperandList()) {
            joinConditionList.add(operand.toString());
        }
        Collections.sort(joinConditionList);
    }

    public boolean joinEquals(Query query) {
        SqlBasicCall condition1 = (SqlBasicCall) this.sqlJoin.getCondition();
        String operator1 = condition1.getOperator().toString();

        SqlBasicCall condition2 = (SqlBasicCall) query.sqlJoin.getCondition();
        String operator2 = condition2.getOperator().toString();

        if (!operator1.equals(operator2) || condition1.getOperandList().size() != condition2.getOperandList().size())
            return false;

        for (int i = 0; i < this.joinConditionList.size(); i++) {
            if (!this.joinConditionList.get(i).equals(query.joinConditionList.get(i))) return false;
        }

        return true;
    }
}