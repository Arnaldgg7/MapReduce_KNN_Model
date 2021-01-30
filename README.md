# MapReduce_KNN_Model
A MapReduce implementation of a KNN predictive model with K=1, aimed at finding the type of tumor (benign or malign) based on several features of the tumor itself.

This implementation is based on 3 MapReduce jobs, each of them focused on optimize and distribute properly the workload in a distributed envionment such as HDFS. The overall goal is to know if we are good enough at predicting cancer tumors by factoring in the existing characteristics measured in each tumor and using a 1NN Predictive Model, by means of the Euclidean Distance as Statistical Test.

The final output (made of trainType_testType types of tumors, which is accessible in the folter MapReduce3.out) provides us with the Confusion Matrix values.

Therefore, we were able to predict the type of the tumor 94.4% of the times ((94 B_B + 57 M_M)/160).
