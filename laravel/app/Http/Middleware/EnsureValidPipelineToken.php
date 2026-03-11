<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

class EnsureValidPipelineToken
{
    public function handle(Request $request, Closure $next): Response
    {
        $expectedToken = config('services.pipeline.ingest_token');
        $providedToken = $request->header('X-Pipeline-Token');

        if (! $expectedToken || ! hash_equals($expectedToken, (string) $providedToken)) {
            abort(Response::HTTP_UNAUTHORIZED);
        }

        return $next($request);
    }
}
