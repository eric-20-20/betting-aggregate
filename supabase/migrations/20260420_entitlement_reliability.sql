create table if not exists public.entitlement_reconciliation_runs (
  id bigint generated always as identity primary key,
  trigger_source text not null check (trigger_source in ('manual', 'scheduled', 'system')),
  status text not null default 'running' check (status in ('running', 'completed', 'failed')),
  total_whop_memberships integer not null default 0,
  total_subscription_rows integer not null default 0,
  total_entitlement_rows integer not null default 0,
  mismatch_count integer not null default 0,
  unresolved_count integer not null default 0,
  initiated_by text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  error_message text,
  summary jsonb not null default '{}'::jsonb
);

create index if not exists entitlement_reconciliation_runs_started_at_idx
  on public.entitlement_reconciliation_runs (started_at desc);

create table if not exists public.entitlement_reconciliation_items (
  id bigint generated always as identity primary key,
  run_id bigint not null references public.entitlement_reconciliation_runs(id) on delete cascade,
  item_key text not null,
  mismatch_type text not null,
  severity text not null default 'warning' check (severity in ('info', 'warning', 'critical')),
  entity_type text not null check (entity_type in ('membership', 'user')),
  whop_user_id text,
  whop_membership_id text,
  expected_access boolean,
  actual_access boolean,
  details jsonb not null default '{}'::jsonb,
  status text not null default 'open' check (status in ('open', 'resolved', 'auto_resolved')),
  resolved_at timestamptz,
  resolved_by text,
  resolution_note text,
  created_at timestamptz not null default now()
);

create unique index if not exists entitlement_reconciliation_items_run_id_item_key_idx
  on public.entitlement_reconciliation_items (run_id, item_key);

create index if not exists entitlement_reconciliation_items_status_created_at_idx
  on public.entitlement_reconciliation_items (status, created_at desc);

create index if not exists entitlement_reconciliation_items_whop_user_id_idx
  on public.entitlement_reconciliation_items (whop_user_id);

create table if not exists public.entitlement_admin_actions (
  id bigint generated always as identity primary key,
  whop_user_id text not null,
  action text not null check (action in ('grant', 'revoke')),
  reason text not null,
  actor_identifier text not null,
  created_at timestamptz not null default now(),
  entitlement_source_before text,
  entitlement_source_after text,
  had_access_before boolean,
  has_access_after boolean,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists entitlement_admin_actions_whop_user_id_created_at_idx
  on public.entitlement_admin_actions (whop_user_id, created_at desc);
