### Things I added/changed (Ale):
1) I compared  each model results against a baseline prediction (predicting that the demand will be equal to how it was 1 hour prior, or equal to how it was yesterday at the same hour): if the naive prediction performs better, it means the trained model literally does not learn anything useful. In our case, All models performed better than the baselines.
2) Trained a new gbtRegressor (taxi-demand-gbt.py); it's basically Xgboost but native to Spark ml, so it should be a better fit overall
3) I added a script (analyse-results.py) that does a more in depth analysis of how the models performed on different zones, and at different hours; I wanted to find some weaknesses and check if they beat the baseline specifically on zones/hours where the demannd is higher or more unpredictible. Another interesting thing that aids our project is the discovery that 2 of the hardest to predict zones are in fact, where the airports are located.-> highlights the importance of adding the fligths data into the equation
4) Random forest performed the best out of the 3 models (even though xgb was close) so i went further with it and tried to improve it by adding a few modifications (in taxi-demand-rf-v3):
- lag_2h and lag_3h, and roll_mean_3h (avg demand over the past 3 hours), to add more context and capture momentum/trends in the last few hours;
- exact direction of the momentum (diff_1h = lag_1h - lag_2h) is the demand going down or up?
- do_lag_1h and  - no. of dropoffs 1 prior in the respective zone (the logic is: people that got dropped there might need a taxi to return a few hours later; also the drop offs bring empty taxis in that zone)


 - I'm conflicted regarding the necesity of using v3 over the original random forest. Over all it seems it doesn't perform better than the original (altough the numbers are veeery close), but it slighly beats it on the harder zones/hours. A downfall is that v3 has more computations due to the added features (i tried to optimize it slightly, but it still took longer than i expected to run locally). The performance shouldn't be that big of a hindrance when running on the cluster, i hope.

5) I also checked the rf-v3 methodology against another random month (march 2026): it still beat both baselines on all metrics and it performed similarly to the july model (slightly worse, but so did the baseline).
6) I tried also running the v3 on a full year of data (the parameters left in v3 are for the full year run) but the program crashed, and i suspect it might be too heavy to run on my machine, so not necessarily the code caused the problem (altough it could be both :p ). We'll have to try to see how it does on the cluster.

7) PS: results are inside the models folder, but the good scripts are outside. rf-v2 doesn't exist because it's basically just a worse v3, it wasn't relevant enough to include in the repo. Sorry for the mess!



