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

    Query(String sql, Planner planner) throws SqlParseException, ValidationException {
        this.sql = sql;
        this.sqlNode = planner.validate(planner.parse(sql));
        this.nodeList = ((SqlSelect)sqlNode).getOperandList();
        this.selectList = (SqlNodeList) this.nodeList.get(1);
        this.sqlJoin = (SqlJoin) nodeList.get(2);
        this.sqlWhere = (SqlBasicCall) nodeList.get(3);
    }

    public boolean joinEquals(Query query){
        SqlBasicCall condition1 = (SqlBasicCall) this.sqlJoin.getCondition();
        String operator1 = condition1.getOperator().toString();
        List<String> operandList1 = new ArrayList<>();

        SqlBasicCall condition2 = (SqlBasicCall) query.sqlJoin.getCondition();
        String operator2 = condition2.getOperator().toString();
        List<String> operandList2 = new ArrayList<>();

        if(!operator1.equals(operator2) || condition1.getOperandList().size() != condition2.getOperandList().size())    return false;

        for(SqlNode operand : condition1.getOperandList()){ operandList1.add(operand.toString()); }
        for(SqlNode operand : condition2.getOperandList()){ operandList2.add(operand.toString()); }

        Collections.sort(operandList1);
        Collections.sort(operandList2);

        for(int i = 0; i < operandList1.size(); i++){
            if(!operandList1.get(i).equals(operandList2.get(i)))    return false;
        }

        return true;
    }
}