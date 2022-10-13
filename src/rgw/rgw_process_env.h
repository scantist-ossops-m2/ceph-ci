// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>

class ActiveRateLimiter;
class OpsLogSink;
class RGWREST;

namespace rgw {
class SiteConfig;
namespace auth {
  class StrategyRegistry;
}
namespace lua {
  class Background;
}
namespace sal {
  class Store;
  class ConfigStore;
  class LuaManager;
}
} // namespace rgw

struct RGWLuaProcessEnv {
  std::string luarocks_path;
  rgw::lua::Background* background = nullptr;
  std::unique_ptr<rgw::sal::LuaManager> manager;
};

struct RGWProcessEnv {
  RGWLuaProcessEnv lua;
  rgw::sal::Store* store = nullptr;
  rgw::sal::ConfigStore* cfgstore = nullptr;
  const rgw::SiteConfig* site = nullptr;
  RGWREST *rest = nullptr;
  OpsLogSink *olog = nullptr;
  std::unique_ptr<rgw::auth::StrategyRegistry> auth_registry;
  ActiveRateLimiter* ratelimiting = nullptr;
};
