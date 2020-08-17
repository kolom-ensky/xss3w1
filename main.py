import numpy as np
import pandas as pd
from lib_main import config
from lib_main import scoring

if __name__ == '__main__':
    # getting data
    DataFrame = scoring.getFreshData(config.CREDENTIALS,'findcsystem')
    
    # getting a list of unique agents
    unique_assignee = np.unique(DataFrame.assignee_id)
    
    # creating a df with scores by statuses
    all_result = pd.DataFrame()
    for assignee in unique_assignee:
        test_user = pd.DataFrame()
        test_result = pd.DataFrame()
        test_user = DataFrame[DataFrame.assignee_id == assignee][:]
        test_user.reset_index(inplace=True, drop=True)
        test_result = scoring.workloadScoringByStatuses(test_user,63,7)[:]
        all_result = pd.concat([all_result, test_result], ignore_index=True)[:]

    # creating a df with mean_score
    all_result_mean = scoring.unionScoring(all_result)
    
    # inserting data to sql base
    scoring.insertScoreResultData_univ(all_result,'findcsystem','xsolla_summer_school',
    'score_result_total',['int64','str','int64','float','float','int64'])
    
    scoring.insertScoreResultData_univ(all_result_mean,'findcsystem','xsolla_summer_school',
    'score_result_total',['int64','float'])
