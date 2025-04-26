import React from "react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

/*───────────────────────────────────────────────
  T2D Pulse – Merged Sector Sentiment Card
  One card per sector (no separate score tiles).
  Score is prominent on the right; card color remains neutral slate.
───────────────────────────────────────────────*/

const RAW = [
  { sector: "SMB SaaS", score: -0.28, tickers: ["BILL", "PAYC", "DDOG"], drivers: ["NASDAQ −4.6%", "Dev‑jobs −8.8%"] },
  { sector: "Enterprise SaaS", score: -0.28, tickers: ["CRM", "NOW", "ADBE"], drivers: ["10‑Yr 4.42%", "NASDAQ weak"] },
  { sector: "Cloud Infra", score: -0.22, tickers: ["AMZN", "MSFT", "GOOGL"], drivers: ["PPI +9%", "10‑Yr headwind"] },
  { sector: "AdTech", score: -0.30, tickers: ["TTD", "PUBM", "GOOGL"], drivers: ["VIX 32.6 spike", "NASDAQ −4.6%", "Ads first to cut"] },
  { sector: "Fintech", score: -0.27, tickers: ["SQ", "PYPL", "ADYEY"], drivers: ["Rates 4.4%", "Consumer +5.3%"] },
  { sector: "Consumer Internet", score: -0.28, tickers: ["META", "GOOGL", "PINS"], drivers: ["VIX spike", "Ad spend risk"] },
  { sector: "eCommerce", score: -0.16, tickers: ["AMZN", "SHOP", "SE"], drivers: ["Real PCE +5.3%", "NASDAQ weak"] },
  { sector: "Cybersecurity", score: -0.26, tickers: ["PANW", "FTNT", "CRWD"], drivers: ["Dev‑jobs soft", "Rates headwind"] },
  { sector: "Dev Tools / Analytics", score: -0.28, tickers: ["SNOW", "DDOG", "ESTC"], drivers: ["Dev‑jobs −8.8%", "NASDAQ slump"] },
  { sector: "Semiconductors", score: -0.11, tickers: ["NVDA", "AMD", "AVGO"], drivers: ["PPI +9%", "10‑Yr headwind"] },
  { sector: "AI Infrastructure", score: -0.22, tickers: ["NVDA", "AMD", "SMCI"], drivers: ["PPI +9%", "Rates headwind"] },
  { sector: "Vertical SaaS", score: -0.28, tickers: ["VEEV", "TYL", "WDAY"], drivers: ["NASDAQ slump", "Dev‑jobs soft"] },
  { sector: "IT Services / Legacy Tech", score: -0.16, tickers: ["IBM", "ACN", "DXC"], drivers: ["Rates headwind", "GDP +2.5%"] },
  { sector: "Hardware / Devices", score: -0.02, tickers: ["AAPL", "DELL", "HPQ"], drivers: ["PPI +9%", "Rates drag"] }
];

const DATA = RAW.map((d) => ({
  ...d,
  stance: d.score <= -0.25 ? "Bearish" : d.score >= 0.05 ? "Bullish" : "Neutral",
  takeaway: d.score <= -0.25 ? "Bearish macro setup" : d.score >= 0.05 ? "Outperforming peers" : "Neutral – monitor prints"
}));

// palette helpers
const accent = (v) => (v >= 0.05 ? "text-cyan-300" : v <= -0.25 ? "text-rose-300" : "text-slate-100");
const badgeColors = (s) =>
  s === "Bullish" ? "bg-cyan-600/20 text-cyan-300" : s === "Bearish" ? "bg-rose-600/30 text-rose-300" : "bg-slate-600 text-slate-200";

export default function SectorSentimentWidget() {
  return (
    <div className="max-h-[90vh] overflow-auto pr-1 grid xl:grid-cols-2 lg:grid-cols-2 md:grid-cols-1 gap-5">
      {DATA.map((d) => (
        <div key={d.sector} className="flex flex-col gap-3 p-5 rounded-xl border border-slate-600 bg-slate-800/70 backdrop-blur shadow-sm">
          {/* header */}
          <div className="flex items-center justify-between">
            <span className="text-lg font-medium text-slate-100">{d.sector}</span>
            <span className={cn("text-2xl font-bold", accent(d.score))}>{d.score > 0 && "+"}{d.score.toFixed(2)}</span>
          </div>

          <Badge className={cn("px-3 py-1 self-start", badgeColors(d.stance))}>{d.stance}</Badge>
          <p className="text-sm text-slate-300 leading-snug">{d.takeaway}</p>

          {/* drivers */}
          <ul className="list-disc list-inside text-sm text-slate-200 space-y-0.5 pl-1">
            {d.drivers.map((dr, idx) => <li key={idx}>{dr}</li>)}
          </ul>

          {/* tickers */}
          <div className="flex flex-wrap gap-2 pt-2">
            {d.tickers.map((t) => (
              <Badge key={t} className="bg-slate-600 text-slate-200 px-2 py-0.5 text-xs rounded">
                {t}
              </Badge>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
