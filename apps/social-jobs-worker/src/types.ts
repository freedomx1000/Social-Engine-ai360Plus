export type SocialJobRow = {
  id: string;
  org_id: string;
  lead_id: string | null;
  activity_id: string | null;
  job_type: string;
  status: string;
  attempts: number | null;
  max_attempts: number | null;
  payload: any;
  locked_at: string | null;
  locked_by: string | null;
  created_at: string;
  updated_at: string;
};

export type CrmLeadActivityRow = {
  id: string;
  org_id: string;
  lead_id: string;
  kind: string;
  message: string | null;
  meta: any;
  payload: any;
  created_at: string;
  type: string | null;
};

export type SocialOutputInsert = {
  org_id: string;
  lead_id: string | null;
  vertical_key?: string | null;
  status?: string;
  channel?: string;
  title?: string | null;
  caption?: string | null;
  hashtags?: string[] | null;
  hook?: string | null;
  cta?: string | null;
  image_prompts?: any; // jsonb
  assets?: any; // jsonb
  meta?: any; // jsonb
};
