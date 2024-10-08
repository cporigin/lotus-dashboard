from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, DECIMAL, Date
from lotus_dashboard.database import Base


class UserPerformance(Base):
    __tablename__ = "user_performance"

    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, index=True)
    lead_created_at = Column(DateTime, index=True)
    deal_id = Column(Integer, index=True)
    deal_code = Column(String(255), index=True)
    group_id = Column(Integer, index=True)
    state = Column(Integer, index=True)
    loi_status = Column(String(255), index=True)
    deal_created_at = Column(DateTime, index=True)
    deal_updated_at = Column(DateTime, index=True)
    deal_task_id = Column(Integer, index=True)
    assigned_group = Column(Integer, index=True)
    task_id = Column(Integer, index=True)
    task_group_id = Column(Integer, index=True)
    task_due_date = Column(Date, index=True)
    deal_task_status = Column(String(255), index=True)
    task_status = Column(String(255), index=True)
    task_created_at = Column(DateTime, index=True)
    task_updated_at = Column(DateTime, index=True)
    deal_comment_id = Column(Integer, index=True)
    deal_comment_user_id = Column(Integer, index=True)
    text = Column(Text)
    status = Column(String(255), index=True)
    comment_created_at = Column(DateTime, index=True)
    time_close_deal_task = Column(DECIMAL(10, 6), index=True)
    time_first_activity = Column(DECIMAL(10, 6), index=True)
    time_first_contacted = Column(DECIMAL(10, 6), index=True)
    time_doing_task = Column(DECIMAL(10, 6), index=True)
    is_recent_task_on_deal = Column(Boolean, index=True)
    group_name = Column(String(255), index=True)
    code = Column(String(255), index=True)
    type = Column(String(255), index=True)
    region = Column(String(255), index=True)
    area_code = Column(String(255), index=True)
    province = Column(String(255), index=True)
    user_comment_last_active_date = Column(Date, index=True)
    first_name = Column(String(255), index=True)
    last_name = Column(String(255), index=True)
    user_comment_created_at = Column(DateTime, index=True)
    user_comment_first_last = Column(String(255), index=True)
