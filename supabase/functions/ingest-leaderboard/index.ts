// Supabase Edge Function: ingest-leaderboard
// POST body: { discord_user_id, username, game_slug, mmr, wins, losses, trophies }
// Auth: x-bot-secret header must match BOT_INGEST_SECRET (set in Lovable Cloud / Supabase secrets)
// Upserts into leaderboard_players on (discord_user_id, game_slug)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-bot-secret",
};

interface IngestBody {
  discord_user_id: string;
  username: string;
  game_slug: string;
  mmr: number;
  wins: number;
  losses: number;
  trophies?: number;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return new Response(
      JSON.stringify({ error: "Method not allowed" }),
      { status: 405, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  const secret = Deno.env.get("BOT_INGEST_SECRET");
  const provided = req.headers.get("x-bot-secret");
  if (!secret || provided !== secret) {
    return new Response(
      JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  let body: IngestBody;
  try {
    body = await req.json();
  } catch {
    return new Response(
      JSON.stringify({ error: "Invalid JSON body" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  const { discord_user_id, username, game_slug, mmr, wins, losses, trophies = 0 } = body;
  if (
    discord_user_id == null ||
    game_slug == null ||
    typeof mmr !== "number" ||
    typeof wins !== "number" ||
    typeof losses !== "number"
  ) {
    return new Response(
      JSON.stringify({ error: "Missing or invalid fields: discord_user_id, game_slug, mmr, wins, losses required" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL");
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!supabaseUrl || !serviceRoleKey) {
    return new Response(
      JSON.stringify({ error: "Server configuration error" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);
  const row = {
    discord_user_id: String(discord_user_id),
    username: String(username ?? ""),
    game_slug: String(game_slug),
    mmr: Number(mmr),
    wins: Number(wins),
    losses: Number(losses),
    trophies: Number(trophies ?? 0),
  };

  const { error } = await supabase
    .from("leaderboard_players")
    .upsert(row, { onConflict: "discord_user_id,game_slug" });

  if (error) {
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  return new Response(
    JSON.stringify({ ok: true }),
    { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
  );
});
