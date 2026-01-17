import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import './index.css'
import { Home } from './pages/Home'
import { GetStarted } from './pages/GetStarted'
import { Analytics } from '@vercel/analytics/react'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Analytics/>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/get-started" element={<GetStarted />} />
      </Routes>
    </BrowserRouter>
  </StrictMode>,
)
