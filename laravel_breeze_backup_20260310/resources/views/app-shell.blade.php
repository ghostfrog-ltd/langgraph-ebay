<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>GhostFrog App</title>

        <style>
            :root {
                --bg: #181818;
                --sidebar: #0f0f10;
                --surface: #222223;
                --surface-soft: #1b1b1c;
                --line: rgba(255, 255, 255, 0.08);
                --text: #f3f3f4;
                --muted: #9a9aa0;
                --chip: #2a2a2c;
            }

            * { box-sizing: border-box; }

            body {
                margin: 0;
                min-height: 100vh;
                background: #1a1a1b;
                color: var(--text);
                font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            }

            a {
                color: inherit;
                text-decoration: none;
            }

            .layout {
                display: grid;
                grid-template-columns: 274px 1fr;
                min-height: 100vh;
            }

            .sidebar {
                background: linear-gradient(180deg, #101011 0%, #0d0d0e 100%);
                border-right: 1px solid var(--line);
                padding: 20px 16px;
                display: flex;
                flex-direction: column;
                gap: 20px;
            }

            .brand {
                display: flex;
                align-items: center;
                gap: 12px;
            }

            .brand-mark {
                width: 34px;
                height: 34px;
                border-radius: 10px;
                display: grid;
                place-items: center;
                background: #fafafa;
                color: #111;
                font-weight: 700;
                font-size: 15px;
            }

            .brand-copy strong {
                display: block;
                font-size: 15px;
                font-weight: 600;
            }

            .section-label {
                color: var(--muted);
                font-size: 13px;
                margin: 12px 4px 8px;
            }

            .nav-link {
                display: flex;
                align-items: center;
                gap: 12px;
                padding: 10px 14px;
                border-radius: 12px;
                color: #efefef;
                font-size: 14px;
            }

            .nav-link.active {
                background: linear-gradient(90deg, #2a2a2d 0%, #222224 100%);
            }

            .nav-icon {
                width: 18px;
                height: 18px;
                border: 1.5px solid currentColor;
                border-radius: 4px;
                opacity: 0.95;
            }

            .sidebar-spacer {
                flex: 1;
            }

            .footer-links {
                display: grid;
                gap: 10px;
                padding: 0 4px;
            }

            .footer-link {
                display: flex;
                align-items: center;
                gap: 10px;
                color: #dddddf;
                font-size: 14px;
            }

            .footer-link-icon {
                width: 16px;
                height: 16px;
                border: 1.5px solid currentColor;
                border-radius: 4px;
                opacity: 0.9;
            }

            .account-dock {
                position: relative;
                margin-top: 18px;
            }

            .account-dock summary {
                list-style: none;
            }

            .account-dock summary::-webkit-details-marker {
                display: none;
            }

            .account-button {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                padding: 12px;
                border-radius: 14px;
                background: #171718;
                border: 1px solid var(--line);
                cursor: pointer;
            }

            .account-main {
                display: flex;
                align-items: center;
                gap: 12px;
                min-width: 0;
            }

            .avatar {
                width: 32px;
                height: 32px;
                border-radius: 10px;
                background: #5e5f61;
                display: grid;
                place-items: center;
                font-size: 14px;
                font-weight: 700;
            }

            .avatar.large {
                width: 42px;
                height: 42px;
                border-radius: 999px;
                font-size: 16px;
            }

            .account-copy {
                min-width: 0;
            }

            .account-copy strong {
                display: block;
                font-size: 14px;
                font-weight: 600;
            }

            .account-copy span {
                display: block;
                color: var(--muted);
                font-size: 13px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }

            .caret {
                width: 8px;
                height: 8px;
                border-right: 1.5px solid #d9d9dc;
                border-bottom: 1.5px solid #d9d9dc;
                transform: rotate(45deg);
                margin-right: 4px;
            }

            .account-menu {
                position: absolute;
                left: 0;
                right: 0;
                bottom: calc(100% + 10px);
                background: #323234;
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 24px;
                padding: 16px;
                box-shadow: 0 24px 70px rgba(0, 0, 0, 0.45);
            }

            .menu-head {
                display: flex;
                align-items: center;
                gap: 12px;
                padding-bottom: 14px;
                border-bottom: 1px solid rgba(255,255,255,0.12);
            }

            .menu-group {
                display: grid;
                gap: 4px;
                padding-top: 14px;
                margin-top: 14px;
                border-top: 1px solid rgba(255,255,255,0.12);
            }

            .menu-link,
            .menu-button {
                width: 100%;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                padding: 12px 8px;
                border-radius: 14px;
                border: 0;
                background: transparent;
                color: var(--text);
                font: inherit;
                text-align: left;
                cursor: pointer;
            }

            .menu-link:hover,
            .menu-button:hover {
                background: rgba(255,255,255,0.05);
            }

            .menu-item-left {
                display: flex;
                align-items: center;
                gap: 12px;
            }

            .menu-icon {
                width: 18px;
                height: 18px;
                border: 1.5px solid currentColor;
                border-radius: 5px;
                opacity: 0.95;
            }

            .main {
                padding: 20px 22px 30px;
                background: #1d1d1e;
            }

            .canvas {
                border-left: 1px solid rgba(255,255,255,0.02);
                padding-left: 12px;
            }

            .panel-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 16px;
                margin-top: 18px;
            }

            .panel,
            .panel-wide {
                position: relative;
                overflow: hidden;
                background: var(--surface);
                border: 1px solid rgba(255,255,255,0.07);
                border-radius: 18px;
            }

            .panel {
                min-height: 316px;
            }

            .panel-wide {
                min-height: 540px;
                margin-top: 16px;
            }

            .panel::before,
            .panel-wide::before {
                content: "";
                position: absolute;
                inset: 0;
                background: repeating-linear-gradient(
                    -45deg,
                    rgba(255,255,255,0.03),
                    rgba(255,255,255,0.03) 1px,
                    transparent 1px,
                    transparent 8px
                );
                pointer-events: none;
            }

            .panel-label {
                position: relative;
                z-index: 1;
                padding: 18px;
                color: #d7d7da;
                font-size: 14px;
            }

            .toolbar {
                display: flex;
                justify-content: flex-end;
                align-items: center;
                gap: 10px;
                margin-bottom: 10px;
            }

            .chip {
                padding: 8px 12px;
                border-radius: 999px;
                border: 1px solid var(--line);
                background: var(--chip);
                color: var(--muted);
                font-size: 13px;
            }

            @media (max-width: 1100px) {
                .panel-grid {
                    grid-template-columns: 1fr;
                }
            }

            @media (max-width: 920px) {
                .layout {
                    grid-template-columns: 1fr;
                }

                .sidebar {
                    display: none;
                }

                .main {
                    padding: 16px;
                }

                .canvas {
                    padding-left: 0;
                    border-left: 0;
                }
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
                    <div class="brand-mark">gf</div>
                    <div class="brand-copy">
                        <strong>GhostFrog</strong>
                    </div>
                </div>

                <div>
                    <div class="section-label">Platform</div>
                    <a class="nav-link active" href="{{ route('app.shell') }}">
                        <span class="nav-icon"></span>
                        <span>Dashboard</span>
                    </a>
                </div>

                <div class="sidebar-spacer"></div>

                <div class="footer-links">
                    <a class="footer-link" href="#">
                        <span class="footer-link-icon"></span>
                        <span>Repository</span>
                    </a>
                    <a class="footer-link" href="#">
                        <span class="footer-link-icon"></span>
                        <span>Documentation</span>
                    </a>
                </div>

                <details class="account-dock">
                    <summary class="account-button">
                        <div class="account-main">
                            <div class="avatar">{{ strtolower($initials ?: 'gf') }}</div>
                            <div class="account-copy">
                                <strong>{{ $user->name }}</strong>
                                <span>ghost frog</span>
                            </div>
                        </div>
                        <span class="caret"></span>
                    </summary>

                    <div class="account-menu">
                        <div class="menu-head">
                            <div class="avatar large">{{ $initials ?: 'G' }}</div>
                            <div class="account-copy">
                                <strong>{{ $user->name }}</strong>
                                <span>{{ $user->email }}</span>
                            </div>
                        </div>

                        <div class="menu-group">
                            <a class="menu-link" href="#">
                                <span class="menu-item-left">
                                    <span class="menu-icon"></span>
                                    <span>Upgrade plan</span>
                                </span>
                            </a>
                            <a class="menu-link" href="#">
                                <span class="menu-item-left">
                                    <span class="menu-icon"></span>
                                    <span>Personalization</span>
                                </span>
                            </a>
                            <a class="menu-link" href="{{ route('profile.edit') }}">
                                <span class="menu-item-left">
                                    <span class="menu-icon"></span>
                                    <span>Settings</span>
                                </span>
                            </a>
                        </div>

                        <div class="menu-group">
                            <a class="menu-link" href="mailto:info@ghostfrog.co.uk">
                                <span class="menu-item-left">
                                    <span class="menu-icon"></span>
                                    <span>Help</span>
                                </span>
                            </a>
                            <form method="POST" action="{{ route('logout') }}">
                                @csrf
                                <button class="menu-button" type="submit">
                                    <span class="menu-item-left">
                                        <span class="menu-icon"></span>
                                        <span>Log out</span>
                                    </span>
                                </button>
                            </form>
                        </div>
                    </div>
                </details>
            </aside>

            <main class="main">
                <div class="canvas">
                    <div class="toolbar">
                        <div class="chip">Admin</div>
                        <a class="chip" href="/">Back to site</a>
                    </div>

                    <div class="panel-grid">
                        <section class="panel">
                            <div class="panel-label">Overview</div>
                        </section>
                        <section class="panel">
                            <div class="panel-label">Signals</div>
                        </section>
                        <section class="panel">
                            <div class="panel-label">Alerts</div>
                        </section>
                    </div>

                    <section class="panel-wide">
                        <div class="panel-label">GhostFrog admin workspace</div>
                    </section>
                </div>
            </main>
        </div>
    </body>
</html>
