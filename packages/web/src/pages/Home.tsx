import { Navbar } from '../components/Navbar';
import { motion } from 'framer-motion';
import { ArrowRight, Zap, ShieldCheck, Cpu, Code2 } from 'lucide-react';
import { CodeBlock } from '../components/CodeBlock';
import { Link } from 'react-router-dom';

const features = [
    {
        icon: <Zap className="w-6 h-6 text-yellow-400" />,
        title: "High Throughput",
        description: "Built for speed with Redis pipelines and optimized Lua scripts."
    },
    {
        icon: <Cpu className="w-6 h-6 text-blue-400" />,
        title: "Low Overhead",
        description: "Minimal memory footprint designed for high-concurrency Node.js apps."
    },
    {
        icon: <ShieldCheck className="w-6 h-6 text-green-400" />,
        title: "Reliable",
        description: "Granular retries, Dead Letter Queues (DLQ), and automatic reclaiming."
    },
    {
        icon: <Code2 className="w-6 h-6 text-purple-400" />,
        title: "TypeScript First",
        description: "Written in TypeScript with full type definitions included."
    }
];

export const Home = () => {
    const previewCode = `// It's this simple
const producer = new Producer(redis, 'stream');
await producer.push({ hello: 'world' }, ['group-A']);

// ... elsewhere
const worker = new Worker(redis, 'group-A', 'stream');
await worker.start();`;

    return (
        <div className="min-h-screen bg-background text-foreground selection:bg-white/20">
            <Navbar />

            <main className="pt-24 pb-16">
                <h1 className="sr-only">Redis Highway - High Performance Redis Queue for Node.js</h1>

                {/* Hero Section */}
                <section className="container mx-auto px-6 py-20 text-center relative overflow-hidden">
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] bg-white/5 rounded-full blur-[120px] -z-10" />

                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5 }}
                        className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-white/5 border border-white/10 text-sm text-muted-foreground mb-8"
                    >
                        <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                        Open Source
                    </motion.div>

                    <motion.h2
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5, delay: 0.1 }}
                        className="text-5xl md:text-7xl font-bold mb-6 tracking-tight"
                    >
                        The <span className="text-transparent bg-clip-text bg-gradient-to-b from-white to-white/40">Highway</span> for<br />
                        your Redis Queues
                    </motion.h2>

                    <motion.p
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5, delay: 0.2 }}
                        className="text-lg text-muted-foreground max-w-2xl mx-auto mb-10"
                    >
                        A high-performance, low-overhead Redis stream-based queue for Node.js.
                        Supports Redis and Valkey single instances.
                    </motion.p>

                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5, delay: 0.3 }}
                        className="flex flex-wrap items-center justify-center gap-4"
                    >
                        <Link to="/get-started" className="px-8 py-3 rounded-full bg-white text-black font-semibold hover:bg-white/90 transition-colors flex items-center gap-2">
                            Get Started <ArrowRight className="w-4 h-4" />
                        </Link>
                        <a href="https://github.com/Koala42/redis-highway" className="px-8 py-3 rounded-full bg-white/5 border border-white/10 hover:bg-white/10 transition-colors">
                            View on GitHub
                        </a>
                    </motion.div>
                </section>

                {/* Feature Grid */}
                <section className="container mx-auto px-6 py-12 border-t border-white/5">
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                        {features.map((feature, i) => (
                            <motion.div
                                key={i}
                                initial={{ opacity: 0, y: 20 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ delay: i * 0.1 }}
                                className="p-6 rounded-2xl bg-white/5 border border-white/10 hover:border-white/20 transition-colors"
                            >
                                <div className="mb-4 p-3 rounded-xl bg-white/5 w-fit">
                                    {feature.icon}
                                </div>
                                <h3 className="text-lg font-semibold mb-2">{feature.title}</h3>
                                <p className="text-sm text-muted-foreground">{feature.description}</p>
                            </motion.div>
                        ))}
                    </div>
                </section>

                {/* Quick Snippet */}
                <section className="container mx-auto px-6 py-12">
                    <div className="max-w-2xl mx-auto relative group">
                        <div className="absolute -inset-1 bg-gradient-to-r from-blue-500 to-purple-500 rounded-2xl blur opacity-20 group-hover:opacity-30 transition duration-1000" />
                        <CodeBlock code={previewCode} />
                    </div>
                </section>

            </main>

            <footer className="border-t border-white/10 py-12">
                <div className="container mx-auto px-6 text-center text-muted-foreground text-sm">
                    <p>© {new Date().getFullYear()} Koala42. MIT License.</p>
                </div>
            </footer>
        </div>
    )
}
