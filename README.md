### Execution steps ###

1. Start sbt instance ``sbt run ``
3. Run application: ``runMain  mapReduceZIO.MapReduceMain <program> <no_of_parralel_executions_workers> <absolute_path_to_data_set_folder> <absolute_path_to_output_folder>``

### Examples ###

Task_1:
`` runMain mapReduceZIO.MapReduceMain TASK_1 20 "/Users/Edvinas.Liutvaitis/IdeaProjects/scala-map-reduce-play/src/main/scala/mapReduceZIO/data/clicks" "/Users/Edvinas.Liutvaitis/IdeaProjects/scala-map-reduce-play/src/main/scala/mapReduceZIO/output/total_clicks.csv"``

Task_2:
`` runMain mapReduceZIO.MapReduceMain TASK_2 20 "/Users/Edvinas.Liutvaitis/IdeaProjects/scala-map-reduce-play/src/main/scala/mapReduceZIO/data" "/Users/Edvinas.Liutvaitis/IdeaProjects/scala-map-reduce-play/src/main/scala/mapReduceZIO/output/filtered_clicks.csv"``

---
**NOTE**

All paths need to be absolute

---