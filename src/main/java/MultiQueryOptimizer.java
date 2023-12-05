import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.List;

public class MultiQueryOptimizer {
    public static List<List<Query>> separateQuery(List<Query> queryList) {
        List<List<Query>> batchList = new ArrayList<>();
        int[] queryFlagArray = new int[queryList.size()];
        int index = 0;
        for (int i = 0; i < queryList.size() - 1; i++) {
            if (queryFlagArray[i] == 1) continue;
            for (int j = i + 1; j < queryList.size(); j++) {
                if (queryFlagArray[j] == 0 && queryList.get(i).joinEquals(queryList.get(j))) {
                    if (queryFlagArray[i] == 0) { // if it is the first equal case,
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

    public static RelNode makeGlobalQuery(RelBuilder relBuilder, List<Query> queryList) throws RelConversionException {
        if (queryList.size() == 1) {
            Query query = queryList.get(0);
            Planner planner = query.planner;
            return planner.rel(query.sqlNode).project();
        }else{
            return relBuilder.scan().build();
        }
    }
}
