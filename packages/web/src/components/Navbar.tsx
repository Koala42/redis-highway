import { Github } from 'lucide-react';
import { Link } from 'react-router-dom';


export const Navbar = () => {
    return (
        <nav className="fixed top-0 left-0 right-0 z-50 border-b border-white/10 bg-background/80 backdrop-blur-md">
            <div className="container mx-auto px-6 h-16 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Link to="/" className="text-xl font-bold bg-gradient-to-r from-white to-gray-400 bg-clip-text text-transparent">
                        Redis Highway
                    </Link>
                    <span className="text-xs px-2 py-0.5 rounded-full bg-white/10 text-muted-foreground border border-white/5">
                        v0.2.4
                    </span>
                </div>

                <div className="flex items-center gap-6">
                    <a href="https://github.com/Koala42/redis-highway" target="_blank" rel="noopener noreferrer"
                        className="text-muted-foreground hover:text-white transition-colors">
                        <Github className="w-5 h-5" />
                    </a>
                    <a href="https://www.npmjs.com/package/@koala42/redis-highway" target="_blank" rel="noopener noreferrer"
                        className="text-sm font-medium text-muted-foreground hover:text-white transition-colors">
                        NPM
                    </a>
                </div>
            </div>
        </nav>
    );
};
