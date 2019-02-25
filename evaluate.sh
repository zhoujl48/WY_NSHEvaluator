#!/usr/bin/env bash

# 工作目录
WORK_DIR=/home/zhoujialiang/evaluate/

# 参数
ds_eval=`date -d "-7 days" +%Y-%m-%d`
ds_pred_start=`date -d "$ds_eval -14 days" +%Y-%m-%d`
ds_pred_end=`date -d "$ds_eval -8 days" +%Y-%m-%d`
ds_ban_start=`date -d "$ds_eval -14 days" +%Y-%m-%d`
ds_ban_end=`date -d "$ds_eval -1 days" +%Y-%m-%d`

# 评估主线挂模型
echo /usr/bin/python3 $WORK_DIR/evaluate.py --tablename zhuxiangua --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end
/usr/bin/python3 $WORK_DIR/evaluate.py --tablename zhuxiangua --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end

# 评估三环挂模型
echo /usr/bin/python3 $WORK_DIR/evaluate.py --tablename sanhuangua --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end
/usr/bin/python3 $WORK_DIR/evaluate.py --tablename sanhuangua --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end

# 评估图谱模型
echo /usr/bin/python3 $WORK_DIR/evaluate.py --tablename graph --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end
/usr/bin/python3 $WORK_DIR/evaluate.py --tablename graph --ds_pred_start $ds_pred_start --ds_pred_end $ds_pred_end --ds_ban_start $ds_ban_start --ds_ban_end $ds_ban_end


