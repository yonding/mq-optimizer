import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;
import org.h2.table.Plan;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;
import static org.apache.commons.lang3.StringUtils.length;

public class MultiQueryOptimizer {
    public static List<List<Query>> separateQuery(List<Query> queryList) {
        List<List<Query>> batchList = new ArrayList<>();
        int[] queryFlagArray = new int[queryList.size()];
        int index = 0;
        for (int i = 0; i < queryList.size() - 1; i++) {
            if (queryFlagArray[i] == 1) continue;
            for (int j = i + 1; j < queryList.size(); j++) {
                if (queryFlagArray[j] == 0 && queryList.get(i).joinEquals(queryList.get(j))) {
                    if (queryFlagArray[i] == 0) { // the first equal case
                        batchList.add(new ArrayList<Query>());
                        batchList.get(index).add(queryList.get(i));
                        queryFlagArray[i] = 1;
                    }
                    batchList.get(index).add(queryList.get(j));
                    queryFlagArray[j] = 1;
                }
            }
            if (queryFlagArray[i] == 0) {
                batchList.add(new ArrayList<Query>());
                batchList.get(index).add(queryList.get(i));
            }
            index++;
        }
        return batchList;
    }

    public static RelNode makeGlobalQuery(RelBuilder relBuilder, List<Query> queryList) throws RelConversionException, SqlParseException, ValidationException {
        if (queryList.size() == 1) {
            Query query = queryList.get(0);
            Planner planner = query.planner;
            return planner.rel(query.sqlNode).project(); // 여기에서 어떻게 RelNode가 만들어지는지 보기
        }else{
            // 코드 추가해야 함.
            String globalQuery = queryList.get(0).sql.substring(0, queryList.get(0).sql.indexOf("WHERE ")+6);

            String conditionStr = "";

            for(Query query : queryList){
                conditionStr += "("+query.sql.substring(query.sql.indexOf("WHERE ")+6)+") OR ";
            }
            conditionStr = conditionStr.substring(0, length(conditionStr)-4);

            globalQuery += conditionStr;
            Planner  planner = Frameworks.getPlanner(queryList.get(0).config.build());

            return planner.rel(planner.validate(planner.parse(globalQuery))).project();
        }
    }

    public static SqlBinaryOperator operatorConverter(SqlKind sqlKind){
        switch (sqlKind.toString()) {
            case "EQUALS":
                return EQUALS;
            case "NOT_EQUALS":
                return NOT_EQUALS;
            case "GREATER_THAN":
                return GREATER_THAN;
            case "GREATER_THAN_OR_EQUAL":
                return GREATER_THAN_OR_EQUAL;
            case "LESS_THAN":
                return LESS_THAN;
            case "LESS_THAN_OR_EQUAL":
                return LESS_THAN_OR_EQUAL;
        }
        return EQUALS;
    }
}

