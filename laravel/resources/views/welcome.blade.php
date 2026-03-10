<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>GhostFrog</title>

        <style>
            :root {
                --bg: #f5f1e7;
                --surface: rgba(255, 251, 244, 0.82);
                --card: rgba(255, 255, 255, 0.68);
                --ink: #17231d;
                --muted: #5f6b64;
                --line: rgba(23, 35, 29, 0.11);
                --accent: #d96e2b;
                --accent-2: #26473c;
                --shadow: 0 24px 60px rgba(31, 37, 29, 0.12);
            }

            * { box-sizing: border-box; }

            body {
                margin: 0;
                min-height: 100vh;
                color: var(--ink);
                font-family: Georgia, "Times New Roman", serif;
                background:
                    radial-gradient(circle at top left, rgba(217, 110, 43, 0.14), transparent 28%),
                    radial-gradient(circle at 85% 15%, rgba(38, 71, 60, 0.18), transparent 24%),
                    linear-gradient(180deg, #faf6ef 0%, var(--bg) 100%);
            }

            a { color: inherit; text-decoration: none; }

            .page {
                width: min(1180px, calc(100% - 32px));
                margin: 0 auto;
                padding: 28px 0 42px;
            }

            .nav {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 16px;
                margin-bottom: 22px;
            }

            .brand {
                display: flex;
                align-items: center;
                gap: 14px;
            }

            .mark {
                width: 46px;
                height: 46px;
                border-radius: 14px;
                display: grid;
                place-items: center;
                font-size: 22px;
                font-weight: 700;
                color: #fff8ef;
                background: linear-gradient(135deg, var(--accent), #f0c361);
                box-shadow: var(--shadow);
            }

            .brand h1,
            .hero h2,
            .panel h3,
            .metric strong {
                margin: 0;
                font-weight: 600;
                letter-spacing: -0.03em;
            }

            .brand p,
            .hero p,
            .metric span,
            .metric p,
            .panel p,
            .footer {
                margin: 0;
                color: var(--muted);
            }

            .nav-actions {
                display: flex;
                gap: 12px;
                align-items: center;
            }

            .button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                min-height: 46px;
                padding: 0 18px;
                border-radius: 999px;
                border: 1px solid transparent;
                font-size: 14px;
                font-weight: 600;
            }

            .button-primary {
                color: #fffdf8;
                background: linear-gradient(135deg, #9a3f13, var(--accent));
            }

            .button-secondary {
                background: rgba(255, 255, 255, 0.56);
                border-color: var(--line);
            }

            .hero {
                display: grid;
                grid-template-columns: 1.55fr 0.95fr;
                gap: 20px;
                margin-bottom: 20px;
            }

            .panel {
                background: var(--surface);
                border: 1px solid var(--line);
                border-radius: 28px;
                box-shadow: var(--shadow);
                backdrop-filter: blur(10px);
            }

            .hero-copy {
                padding: 34px;
            }

            .eyebrow {
                display: inline-flex;
                padding: 8px 12px;
                border-radius: 999px;
                margin-bottom: 18px;
                background: rgba(38, 71, 60, 0.1);
                color: var(--accent-2);
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.12em;
                text-transform: uppercase;
            }

            .hero h2 {
                font-size: clamp(2.8rem, 5.6vw, 5.4rem);
                line-height: 0.93;
                max-width: 9ch;
                margin-bottom: 18px;
            }

            .hero p {
                font-size: 18px;
                line-height: 1.6;
                max-width: 33rem;
            }

            .actions {
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                margin-top: 28px;
            }

            .hero-side {
                padding: 22px;
                display: grid;
                gap: 14px;
            }

            .metric {
                background: var(--card);
                border: 1px solid rgba(23, 35, 29, 0.08);
                border-radius: 22px;
                padding: 18px;
            }

            .metric span {
                display: block;
                margin-bottom: 10px;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.12em;
                text-transform: uppercase;
            }

            .metric strong {
                display: block;
                margin-bottom: 8px;
                font-size: 28px;
            }

            .grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 20px;
            }

            .panel-body {
                padding: 26px;
            }

            .label {
                display: inline-flex;
                padding: 6px 10px;
                border-radius: 999px;
                margin-bottom: 12px;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--accent-2);
                background: rgba(38, 71, 60, 0.1);
            }

            .panel h3 {
                font-size: 30px;
                margin-bottom: 8px;
            }

            .list {
                list-style: none;
                padding: 0;
                margin: 18px 0 0;
                display: grid;
                gap: 12px;
            }

            .list li {
                padding-top: 14px;
                border-top: 1px solid var(--line);
                color: var(--muted);
                line-height: 1.5;
                font-size: 15px;
            }

            .list li:first-child {
                border-top: 0;
                padding-top: 0;
            }

            .footer {
                margin-top: 20px;
                text-align: center;
                font-size: 13px;
            }

            @media (max-width: 920px) {
                .hero, .grid { grid-template-columns: 1fr; }
                .hero h2 { max-width: 11ch; }
                .nav { flex-direction: column; align-items: flex-start; }
            }
        </style>
    </head>
    <body>
        <main class="page">
            <header class="nav">
                <div class="brand">
                    <div class="mark">G</div>
                    <div>
                        <h1>GhostFrog</h1>
                        <p>eBay sourcing intelligence with a SaaS product layer</p>
                    </div>
                </div>

                <div class="nav-actions">
                    <a class="button button-secondary" href="mailto:info@ghostfrog.co.uk">Contact</a>
                    <a class="button button-primary" href="{{ route('login') }}">Login</a>
                </div>
            </header>

            <section class="hero">
                <article class="panel hero-copy">
                    <span class="eyebrow">GhostFrog SaaS Preview</span>
                    <h2>Find the deals before everyone else does.</h2>
                    <p>
                        The Python system keeps scraping, assessing, and pricing listings in the background.
                        Laravel now becomes the customer-facing layer for members, plans, dashboards, and admin control.
                    </p>

                    <div class="actions">
                        <a class="button button-primary" href="{{ route('login') }}">Admin login</a>
                        <a class="button button-secondary" href="{{ route('login') }}">Open protected app</a>
                    </div>
                </article>

                <aside class="panel hero-side">
                    <div class="metric">
                        <span>Access model</span>
                        <strong>Admin only</strong>
                        <p>Public sign-up is disabled. Only seeded admin accounts can log into the app.</p>
                    </div>

                    <div class="metric">
                        <span>Current engine</span>
                        <strong>Python + Postgres</strong>
                        <p>Existing retrieval, comps, ROI, and assessment logic stay in place for now.</p>
                    </div>
                </aside>
            </section>

            <section class="grid">
                <article class="panel">
                    <div class="panel-body">
                        <span class="label">Roles</span>
                        <h3>Single admin role</h3>
                        <p>The first secured version keeps authorization simple: one admin user, one protected app shell.</p>
                        <ul class="list">
                            <li>Login only, no public registration</li>
                            <li>Admin middleware on the workspace route</li>
                            <li>Seeded admin account via env-backed seeder</li>
                        </ul>
                    </div>
                </article>

                <article class="panel">
                    <div class="panel-body">
                        <span class="label">Billing</span>
                        <h3>Cashier ready</h3>
                        <p>Cashier is installed, but billing stays dormant until the user/account model is locked down.</p>
                        <ul class="list">
                            <li>Stripe integration package installed</li>
                            <li>User model is billable</li>
                            <li>Subscription UI comes later</li>
                        </ul>
                    </div>
                </article>

                <article class="panel">
                    <div class="panel-body">
                        <span class="label">App shell</span>
                        <h3>Protected workspace</h3>
                        <p>The left-sidebar app shell is now the destination after login, and only admins can reach it.</p>
                        <ul class="list">
                            <li>Chat-style layout for the future SaaS</li>
                            <li>Persistent workspace navigation</li>
                            <li>Room for listing, ROI, and admin modules</li>
                        </ul>
                    </div>
                </article>
            </section>

            <p class="footer">Auth is now the front door. The next step is creating your admin account and running the reviewed migrations.</p>
        </main>
    </body>
</html>
