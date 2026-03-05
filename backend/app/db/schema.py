ddl_statements = [
    """
    CREATE TABLE IF NOT EXISTS ingestion_log (
      ingestion_id VARCHAR PRIMARY KEY,
      uploaded_at TIMESTAMP,
      file_name VARCHAR,
      file_hash VARCHAR,
      detected_dataset VARCHAR,
      rows_ingested INTEGER,
      status VARCHAR,
      errors VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_alias_map (
      alias_name VARCHAR PRIMARY KEY,
      canonical_name VARCHAR,
      updated_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_company (
      company_id VARCHAR PRIMARY KEY,
      company_name_canonical VARCHAR,
      industry VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_company_variant (
      company_id VARCHAR,
      company_name_raw VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_user (
      user_id VARCHAR PRIMARY KEY,
      contact_key VARCHAR,
      first_name VARCHAR,
      last_name VARCHAR,
      email VARCHAR,
      company_name_raw VARCHAR,
      company_id VARCHAR,
      member_status VARCHAR,
      user_status VARCHAR,
      state VARCHAR,
      country VARCHAR,
      has_photo BOOLEAN,
      has_bio BOOLEAN,
      has_education BOOLEAN,
      has_job_history BOOLEAN,
      mentor_status BOOLEAN,
      mentee_status BOOLEAN,
      volunteer_status BOOLEAN,
      created_at TIMESTAMP,
      last_seen_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_user_activity_snapshot (
      period_start_date DATE,
      period_grain VARCHAR,
      user_id VARCHAR,
      logins INTEGER,
      downloads INTEGER,
      documents_created INTEGER,
      threads_created INTEGER,
      discussion_replies INTEGER,
      replies_to_sender INTEGER,
      blogs_created INTEGER,
      questions_created INTEGER,
      answers_created INTEGER,
      best_answers_discussion INTEGER,
      best_answers_qa INTEGER,
      recommends_given INTEGER,
      follows INTEGER,
      source_upload_id VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_user_activity_lifetime (
      user_id VARCHAR PRIMARY KEY,
      first_activity_date DATE,
      last_activity_date DATE,
      lifetime_logins INTEGER,
      lifetime_downloads INTEGER,
      lifetime_replies INTEGER,
      lifetime_threads INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_login_flag (
      user_id VARCHAR,
      logged_in_since_2024 BOOLEAN,
      last_login_date DATE,
      source_upload_id VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_friend_requests (
      requester_user_id VARCHAR,
      requested_user_id VARCHAR,
      request_status VARCHAR,
      request_date DATE,
      source_upload_id VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_friend_counts (
      user_id VARCHAR,
      active_friendships INTEGER,
      initiated_requests INTEGER,
      received_requests INTEGER,
      pending_requests INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_thread (
      thread_id VARCHAR PRIMARY KEY,
      community_name VARCHAR,
      community_type VARCHAR,
      thread_type VARCHAR,
      subject VARCHAR,
      created_at TIMESTAMP,
      closed_at TIMESTAMP,
      author_user_id VARCHAR,
      author_company_id VARCHAR,
      state VARCHAR,
      country VARCHAR,
      total_replies INTEGER,
      replies_to_thread INTEGER,
      replies_to_sender INTEGER,
      total_recommends INTEGER,
      total_following INTEGER,
      source_upload_id VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_discussion_participation (
      user_id VARCHAR,
      thread_id VARCHAR,
      participation_type VARCHAR,
      count INTEGER,
      period_start_date DATE,
      source_upload_id VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_user_scores_period (
      period_start_date DATE,
      grain VARCHAR,
      user_id VARCHAR,
      engagement_score_0_100 DOUBLE,
      super_user_score_0_100 DOUBLE,
      engagement_tier VARCHAR,
      drivers_json VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_company_health_period (
      period_start_date DATE,
      grain VARCHAR,
      company_id VARCHAR,
      company_health_score_0_100 DOUBLE,
      active_users INTEGER,
      engaged_users INTEGER,
      contributors INTEGER,
      threads INTEGER,
      replies INTEGER,
      downloads INTEGER,
      cross_company_connections INTEGER,
      risk_flags_json VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_topic_catalog (
      topic_id VARCHAR,
      topic_label VARCHAR,
      top_keywords_json VARCHAR,
      model_version VARCHAR,
      created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_thread_topics (
      thread_id VARCHAR,
      topic_id VARCHAR,
      topic_confidence DOUBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_topic_metrics_period (
      period_start_date DATE,
      grain VARCHAR,
      topic_id VARCHAR,
      threads INTEGER,
      replies INTEGER,
      unique_participants INTEGER,
      avg_replies_per_thread DOUBLE,
      company_diversity_index DOUBLE,
      new_engager_rate DOUBLE,
      influence_score DOUBLE,
      lift_vs_baseline DOUBLE,
      best_lag INTEGER,
      lag_corr DOUBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_network_edges_period (
      period_start_date DATE,
      grain VARCHAR,
      from_user_id VARCHAR,
      to_user_id VARCHAR,
      weight DOUBLE,
      interaction_type VARCHAR,
      last_seen_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_network_metrics_user_period (
      period_start_date DATE,
      grain VARCHAR,
      user_id VARCHAR,
      in_degree DOUBLE,
      out_degree DOUBLE,
      pagerank DOUBLE,
      betweenness DOUBLE,
      reciprocity_rate DOUBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_network_metrics_company_period (
      period_start_date DATE,
      grain VARCHAR,
      company_id VARCHAR,
      company_pagerank DOUBLE,
      cross_company_edges INTEGER,
      internal_edges INTEGER,
      isolation_score DOUBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS run_metadata (
      run_id VARCHAR,
      executed_at TIMESTAMP,
      scoring_config_json VARCHAR,
      counts_json VARCHAR,
      timing_json VARCHAR,
      errors VARCHAR
    );
    """,
]
