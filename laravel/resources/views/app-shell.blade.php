<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>GhostFrog App</title>

        <style>
            :root {
                --bg: #1f201d;
                --sidebar: #151613;
                --panel: #20231f;
                --panel-2: #2a2d29;
                --ink: #f1efe8;
                --muted: #9da39a;
                --line: rgba(255, 255, 255, 0.08);
                --accent: #d56d2a;
            }

            * { box-sizing: border-box; }

            body {
                margin: 0;
                min-height: 100vh;
                background:
                    radial-gradient(circle at top center, rgba(213, 109, 42, 0.08), transparent 24%),
                    linear-gradient(180deg, #21231f 0%, var(--bg) 100%);
                color: var(--ink);
                font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            }

            a { color: inherit; text-decoration: none; }

            .layout {
                display: grid;
                grid-template-columns: 280px 1fr;
                min-height: 100vh;
            }

            .sidebar {
                padding: 18px 12px 14px;
                background: linear-gradient(180deg, #111210 0%, var(--sidebar) 100%);
                border-right: 1px solid var(--line);
                display: flex;
                flex-direction: column;
                gap: 14px;
            }

            .brand {
                display: flex;
                align-items: center;
                gap: 12px;
                padding: 6px 10px 12px;
            }

            .logo {
                width: 36px;
                height: 36px;
                border-radius: 12px;
                display: grid;
                place-items: center;
                background: linear-gradient(135deg, var(--accent), #efb45f);
                color: #fff9f0;
                font-weight: 700;
            }

            .brand strong {
                display: block;
                font-size: 15px;
            }

            .brand span,
            .stat p,
            .account-copy span,
            .menu-link small,
            .menu-button small {
                color: var(--muted);
            }

            .sidebar-spacer {
                flex: 1;
                border-radius: 28px;
                background:
                    linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0)),
                    radial-gradient(circle at bottom center, rgba(255,255,255,0.04), transparent 44%);
            }

            .account-dock {
                position: relative;
                margin-top: auto;
            }

            .account-dock summary {
                list-style: none;
            }

            .account-dock summary::-webkit-details-marker {
                display: none;
            }

            .account-card {
                display: flex;
                align-items: center;
                gap: 12px;
                padding: 14px 16px;
                border-radius: 22px;
                border: 1px solid var(--line);
                background: rgba(255,255,255,0.03);
                cursor: pointer;
            }

            .account-card:hover {
                background: rgba(255,255,255,0.05);
            }

            .account-avatar {
                width: 42px;
                height: 42px;
                border-radius: 999px;
                display: grid;
                place-items: center;
                background: #8d9899;
                color: white;
                font-size: 17px;
                font-weight: 700;
                flex: 0 0 auto;
            }

            .account-avatar-large {
                width: 50px;
                height: 50px;
            }

            .account-copy {
                min-width: 0;
            }

            .account-copy strong {
                display: block;
                font-size: 15px;
            }

            .account-copy span {
                display: block;
                font-size: 14px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }

            .account-menu {
                position: absolute;
                left: 0;
                right: 0;
                bottom: calc(100% + 12px);
                padding: 14px;
                border-radius: 30px;
                border: 1px solid rgba(255,255,255,0.1);
                background: #373734;
                box-shadow: 0 26px 80px rgba(0, 0, 0, 0.42);
            }

            .menu-head {
                display: flex;
                align-items: center;
                gap: 14px;
                padding: 6px 6px 16px;
                border-bottom: 1px solid rgba(255,255,255,0.12);
            }

            .menu-section {
                display: grid;
                gap: 4px;
                margin-top: 12px;
                padding-top: 12px;
                border-top: 1px solid rgba(255,255,255,0.12);
            }

            .menu-link,
            .menu-button {
                width: 100%;
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                padding: 13px 10px;
                border-radius: 16px;
                background: transparent;
                border: 0;
                font: inherit;
                text-align: left;
                cursor: pointer;
            }

            .menu-link:hover,
            .menu-button:hover {
                background: rgba(255,255,255,0.05);
            }

            .menu-link span,
            .menu-button span {
                color: #f1efe8;
                font-size: 16px;
            }

            .main {
                padding: 18px 22px 26px;
            }

            .topbar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 16px;
                margin-bottom: 18px;
            }

            .crumb {
                font-size: 22px;
                font-weight: 600;
            }

            .toolbar {
                display: flex;
                gap: 10px;
                align-items: center;
            }

            .pill {
                padding: 10px 14px;
                border-radius: 999px;
                border: 1px solid var(--line);
                background: rgba(255, 255, 255, 0.04);
                color: var(--muted);
                font-size: 14px;
            }

            .hero {
                max-width: 980px;
                margin: 0 auto;
            }

            .canvas {
                border: 1px solid var(--line);
                border-radius: 26px;
                background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
                min-height: 620px;
                padding: 28px;
                display: flex;
                flex-direction: column;
            }

            .intro {
                max-width: 720px;
                margin-bottom: 28px;
            }

            .intro h1 {
                margin: 0 0 12px;
                font-size: clamp(2rem, 4vw, 3.25rem);
                line-height: 1.02;
                letter-spacing: -0.03em;
            }

            .intro p,
            .message p,
            .message li {
                color: #ddd9cc;
                font-size: 18px;
                line-height: 1.7;
            }

            .thought {
                display: inline-flex;
                align-items: center;
                gap: 10px;
                color: var(--muted);
                margin-bottom: 20px;
                font-size: 15px;
            }

            .message {
                max-width: 760px;
            }

            .message ul {
                margin: 10px 0 0 20px;
            }

            .stats {
                margin-top: 26px;
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 14px;
            }

            .stat {
                padding: 18px;
                border-radius: 20px;
                background: rgba(255,255,255,0.04);
                border: 1px solid var(--line);
            }

            .stat strong {
                display: block;
                font-size: 24px;
                margin-bottom: 6px;
            }

            .composer {
                margin-top: auto;
                padding-top: 26px;
            }

            .composer-box {
                display: flex;
                flex-direction: column;
                gap: 14px;
                padding: 18px 18px 14px;
                border-radius: 28px;
                background: var(--panel-2);
                border: 1px solid var(--line);
                max-width: 820px;
            }

            .composer-input {
                font-size: 28px;
                color: #c8c4b7;
            }

            .composer-footer {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
            }

            .left-tools,
            .right-tools {
                display: flex;
                align-items: center;
                gap: 10px;
            }

            .icon-button {
                width: 42px;
                height: 42px;
                display: grid;
                place-items: center;
                border-radius: 999px;
                border: 1px solid var(--line);
                background: rgba(255,255,255,0.04);
            }

            .primary-send {
                background: #f2eee3;
                color: #161714;
                border-color: transparent;
                font-weight: 700;
            }

            @media (max-width: 980px) {
                .layout { grid-template-columns: 1fr; }
                .sidebar { display: none; }
                .stats { grid-template-columns: 1fr; }
                .main { padding: 16px; }
                .canvas { min-height: auto; }
            }
        </style>
    </head>
    <body>
        @php
            $user = auth()->user();
            $initials = collect(explode(' ', trim($user->name)))
                ->filter()
                ->map(fn ($part) => strtoupper(substr($part, 0, 1)))
                ->take(2)
                ->implode('');
        @endphp

        <div class="layout">
            <aside class="sidebar">
                <div class="brand">
                    <div class="logo">G</div>
                    <div>
                        <strong>GhostFrog</strong>
                        <span>SaaS workspace</span>
                    </div>
                </div>

                <div class="sidebar-spacer"></div>

                <details class="account-dock">
                    <summary class="account-card">
                        <div class="account-avatar">{{ $initials ?: 'G' }}</div>
                        <div class="account-copy">
                            <strong>{{ $user->name }}</strong>
                            <span>{{ ucfirst($user->role) }}</span>
                        </div>
                    </summary>

                    <div class="account-menu">
                        <div class="menu-head">
                            <div class="account-avatar account-avatar-large">{{ $initials ?: 'G' }}</div>
                            <div class="account-copy">
                                <strong>{{ $user->name }}</strong>
                                <span>{{ $user->email }}</span>
                            </div>
                        </div>

                        <div class="menu-section">
                            <a class="menu-link" href="#">
                                <span>Upgrade plan</span>
                                <small>Later</small>
                            </a>
                            <a class="menu-link" href="#">
                                <span>Personalization</span>
                                <small>Later</small>
                            </a>
                            <a class="menu-link" href="{{ route('profile.edit') }}">
                                <span>Settings</span>
                                <small>Profile</small>
                            </a>
                        </div>

                        <div class="menu-section">
                            <a class="menu-link" href="mailto:info@ghostfrog.co.uk">
                                <span>Help</span>
                                <small>Email</small>
                            </a>
                            <form method="POST" action="{{ route('logout') }}">
                                @csrf
                                <button class="menu-button" type="submit">
                                    <span>Log out</span>
                                    <small>Exit</small>
                                </button>
                            </form>
                        </div>
                    </div>
                </details>
            </aside>

            <main class="main">
                <div class="topbar">
                    <div class="crumb">GhostFrog Insight Workspace</div>
                    <div class="toolbar">
                        <div class="pill">Local mode</div>
                        <div class="pill">Role: Admin</div>
                        <a class="pill" href="/">Back to site</a>
                    </div>
                </div>

                <section class="hero">
                    <div class="canvas">
                        <div class="intro">
                            <h1>Search, assess, and surface deals in one workspace.</h1>
                            <p>
                                This is the direction for the real SaaS interface: a quiet left rail, a focused working
                                canvas on the right, and room for listing insights, saved searches, ROI views, and
                                admin-only controls.
                            </p>
                        </div>

                        <div class="thought">Live concept shell for the future authenticated app</div>

                        <div class="message">
                            <p>Initial product areas this layout can support:</p>
                            <ul>
                                <li>premium deal feed by niche and subscription tier</li>
                                <li>saved search workspaces with comps and ROI context</li>
                                <li>admin oversight for ingestion quality and user plans</li>
                                <li>member-facing alert history and listing review flow</li>
                            </ul>
                        </div>

                        <div class="stats">
                            <div class="stat">
                                <strong>Feeds</strong>
                                <p>Tier-gated listing streams, curated by source and niche.</p>
                            </div>
                            <div class="stat">
                                <strong>Workspaces</strong>
                                <p>Saved contexts for Apple, Lego, motors, or custom verticals.</p>
                            </div>
                            <div class="stat">
                                <strong>Admin tools</strong>
                                <p>Visibility controls, plan management, and pipeline oversight.</p>
                            </div>
                        </div>

                        <div class="composer">
                            <div class="composer-box">
                                <div class="composer-input">Ask GhostFrog to surface profitable listings</div>
                                <div class="composer-footer">
                                    <div class="left-tools">
                                        <div class="icon-button">+</div>
                                        <div class="pill">Mode: sourcing</div>
                                    </div>
                                    <div class="right-tools">
                                        <div class="pill">{{ $user->name }}</div>
                                        <div class="icon-button">🎤</div>
                                        <div class="icon-button primary-send">Go</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>
            </main>
        </div>
    </body>
</html>
