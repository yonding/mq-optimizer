import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

import java.util.*;

public class Query {
    String sql;
    SqlNode sqlNode;
    List nodeList;

    // 질의 정보
    List selectList;
    SqlJoin sqlJoin;
    SqlBasicCall sqlWhere;

    Planner planner;
    List<String> joinTableList;

    Query(String sql, Planner planner) throws SqlParseException, ValidationException {
        this.sql = sql;
        this.sqlNode = planner.validate(planner.parse(sql));
        this.nodeList = ((SqlSelect) sqlNode).getOperandList();
        this.selectList = (SqlNodeList) this.nodeList.get(1);
        this.sqlJoin = (SqlJoin) nodeList.get(2);
        this.sqlWhere = (SqlBasicCall) nodeList.get(3);
        this.planner = planner;
        this.joinTableList = new ArrayList<>();
        for (SqlNode operand : ((SqlBasicCall)this.sqlJoin.getCondition()).getOperandList()) {
            joinTableList.add(operand.toString());
        }
        Collections.sort(joinTableList);
    }

    public boolean joinEquals(Query query) {
        SqlBasicCall condition1 = (SqlBasicCall) this.sqlJoin.getCondition();
        String operator1 = condition1.getOperator().toString();

        SqlBasicCall condition2 = (SqlBasicCall) query.sqlJoin.getCondition();
        String operator2 = condition2.getOperator().toString();

        if (!operator1.equals(operator2) || condition1.getOperandList().size() != condition2.getOperandList().size())
            return false;

        for (int i = 0; i < this.joinTableList.size(); i++) {
            if (!this.joinTableList.get(i).equals(query.joinTableList.get(i))) return false;
        }

        return true;
    }
}