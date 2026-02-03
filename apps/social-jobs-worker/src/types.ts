export type SocialJobStatus = "queued" | "running" | "done" | "failed";

export type SocialJobType = "generate_assets";

export type SocialJobRow = {
    org_id: string;
  lead_id: string;
  activity_id: string;
  job_type: SocialJobType;
  status: SocialJobStatus;
  attempts: number;
  max_attempts: number;
  payload: any;
  locked_at: string | null;
  locked_by: string | null;
  created_at: string;
  updated_at: string;
};

export type LeadRow = {
  id: string;
  org_id: string;
  email: string | null;
  name: string | null;
  phone: string | null;
  stage: string | null;
  source: string | null;
  tags: string[] | null;
  score: number | null;
  score_updated_at: string | null;
  score_reason: string | null;
  lead_type: string | null;
  vertical: string | null;
  interest_level: string | null;
  last_activity_at: string | null;
  last_activity_kind: string | null;
  created_by: string | null;
  created_at: string;
  deleted_at: string | null;
};

export type LeadActivityRow = {
  id: string;
  org_id: string;
  lead_id: string;
  kind: string | null;
  type: string | null;
  message: string | null;
  meta: any;
  payload: any;
  created_at: string;
};
