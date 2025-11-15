package tweetoscope.kafka;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Web server pour afficher le leaderboard en temps réel
 * Accessible sur http://localhost:8080
 */
public class KafkaLeaderboardWebServer {

    private HttpServer server;
    private static final int PORT = 8080;
    private final Gson gson = new Gson();
    private volatile Map<String, Integer> currentLeaderboard = new LinkedHashMap<>();

    public KafkaLeaderboardWebServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));
        
        // Route: page HTML
        server.createContext("/", new HtmlHandler());
        
        // Route: API JSON
        server.createContext("/api/leaderboard", new ApiHandler());
        
        System.out.println("[WebServer] Starting on http://0.0.0.0:" + PORT);
        server.start();
        System.out.println("[WebServer] Ready for connections");
    }

    public void updateLeaderboard(Map<String, Integer> leaderboard) {
        synchronized (this) {
            this.currentLeaderboard = new LinkedHashMap<>(leaderboard);
        }
    }

    private class HtmlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String html = "<!DOCTYPE html>\n" +
                    "<html lang=\"en\">\n" +
                    "<head>\n" +
                    "    <meta charset=\"UTF-8\">\n" +
                    "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                    "    <title>Tweetoscope Leaderboard</title>\n" +
                    "    <style>\n" +
                    "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                    "        body {\n" +
                    "            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n" +
                    "            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);\n" +
                    "            min-height: 100vh;\n" +
                    "            display: flex;\n" +
                    "            justify-content: center;\n" +
                    "            align-items: center;\n" +
                    "            padding: 20px;\n" +
                    "        }\n" +
                    "        .container {\n" +
                    "            background: white;\n" +
                    "            border-radius: 15px;\n" +
                    "            box-shadow: 0 20px 60px rgba(0,0,0,0.3);\n" +
                    "            max-width: 500px;\n" +
                    "            width: 100%;\n" +
                    "            padding: 40px;\n" +
                    "        }\n" +
                    "        h1 {\n" +
                    "            color: #333;\n" +
                    "            margin-bottom: 10px;\n" +
                    "            text-align: center;\n" +
                    "            font-size: 2em;\n" +
                    "        }\n" +
                    "        .subtitle {\n" +
                    "            color: #999;\n" +
                    "            text-align: center;\n" +
                    "            margin-bottom: 30px;\n" +
                    "            font-size: 0.9em;\n" +
                    "        }\n" +
                    "        .leaderboard {\n" +
                    "            list-style: none;\n" +
                    "        }\n" +
                    "        .leaderboard li {\n" +
                    "            display: flex;\n" +
                    "            align-items: center;\n" +
                    "            padding: 15px;\n" +
                    "            margin-bottom: 10px;\n" +
                    "            background: #f5f5f5;\n" +
                    "            border-radius: 8px;\n" +
                    "            border-left: 4px solid #667eea;\n" +
                    "            transition: all 0.3s ease;\n" +
                    "        }\n" +
                    "        .leaderboard li:hover {\n" +
                    "            background: #efefef;\n" +
                    "            transform: translateX(5px);\n" +
                    "        }\n" +
                    "        .rank {\n" +
                    "            font-weight: bold;\n" +
                    "            color: #667eea;\n" +
                    "            font-size: 1.3em;\n" +
                    "            min-width: 40px;\n" +
                    "            text-align: center;\n" +
                    "        }\n" +
                    "        .hashtag {\n" +
                    "            flex: 1;\n" +
                    "            margin: 0 20px;\n" +
                    "            color: #333;\n" +
                    "            font-weight: 500;\n" +
                    "        }\n" +
                    "        .count {\n" +
                    "            background: #667eea;\n" +
                    "            color: white;\n" +
                    "            padding: 8px 15px;\n" +
                    "            border-radius: 20px;\n" +
                    "            font-weight: bold;\n" +
                    "            min-width: 60px;\n" +
                    "            text-align: center;\n" +
                    "        }\n" +
                    "        .refresh-info {\n" +
                    "            text-align: center;\n" +
                    "            color: #999;\n" +
                    "            font-size: 0.85em;\n" +
                    "            margin-top: 20px;\n" +
                    "        }\n" +
                    "        .loading {\n" +
                    "            text-align: center;\n" +
                    "            color: #999;\n" +
                    "            padding: 40px 20px;\n" +
                    "        }\n" +
                    "    </style>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "    <div class=\"container\">\n" +
                    "        <h1>🏆 Tweetoscope</h1>\n" +
                    "        <div class=\"subtitle\">Top Trending Hashtags</div>\n" +
                    "        <ul class=\"leaderboard\" id=\"leaderboard\">\n" +
                    "            <li class=\"loading\">Loading hashtags...</li>\n" +
                    "        </ul>\n" +
                    "        <div class=\"refresh-info\">Updates every 2 seconds</div>\n" +
                    "    </div>\n" +
                    "\n" +
                    "    <script>\n" +
                    "        async function updateLeaderboard() {\n" +
                    "            try {\n" +
                    "                const response = await fetch('/api/leaderboard');\n" +
                    "                const data = await response.json();\n" +
                    "                const list = document.getElementById('leaderboard');\n" +
                    "                list.innerHTML = '';\n" +
                    "                \n" +
                    "                if (data.leaderboard && Object.keys(data.leaderboard).length > 0) {\n" +
                    "                    let rank = 1;\n" +
                    "                    for (const [hashtag, count] of Object.entries(data.leaderboard)) {\n" +
                    "                        const li = document.createElement('li');\n" +
                    "                        li.innerHTML = `\n" +
                    "                            <div class=\"rank\">#${rank}</div>\n" +
                    "                            <div class=\"hashtag\">${hashtag}</div>\n" +
                    "                            <div class=\"count\">${count.toLocaleString()}</div>\n" +
                    "                        `;\n" +
                    "                        list.appendChild(li);\n" +
                    "                        rank++;\n" +
                    "                    }\n" +
                    "                } else {\n" +
                    "                    const li = document.createElement('li');\n" +
                    "                    li.className = 'loading';\n" +
                    "                    li.textContent = 'Waiting for data...';\n" +
                    "                    list.appendChild(li);\n" +
                    "                }\n" +
                    "            } catch (error) {\n" +
                    "                console.error('Error:', error);\n" +
                    "                const list = document.getElementById('leaderboard');\n" +
                    "                list.innerHTML = '<li class=\"loading\">Error loading data</li>';\n" +
                    "            }\n" +
                    "        }\n" +
                    "\n" +
                    "        // Update on load and every 2 seconds\n" +
                    "        updateLeaderboard();\n" +
                    "        setInterval(updateLeaderboard, 2000);\n" +
                    "    </script>\n" +
                    "</body>\n" +
                    "</html>";

            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, html.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(html.getBytes());
            }
        }
    }

    private class ApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            
            synchronized (currentLeaderboard) {
                String json = gson.toJson(Collections.singletonMap("leaderboard", currentLeaderboard));
                exchange.sendResponseHeaders(200, json.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(json.getBytes());
                }
            }
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }
}
