import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.List;

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
        return true;
    }
}
