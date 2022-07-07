// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LoadRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/crypto/Types.h"
#include "librbd/crypto/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::LoadRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
LoadRequest<I>::LoadRequest(
        I* image_ctx, std::vector<EncryptionFormat>&& formats,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_on_finish(on_finish),
                              m_format_idx(0),
                              m_is_current_format_cloned(false),
                              m_formats(std::move(formats)) {
}

template <typename I>
void LoadRequest<I>::send() {
  ldout(m_image_ctx->cct, 20) << "got " << m_formats.size() << " formats"
                              << dendl;

  m_image_ctx->image_lock.lock_shared();
  bool is_encryption_loaded = m_image_ctx->encryption_format.get() != nullptr;
  m_image_ctx->image_lock.unlock_shared();

  if (is_encryption_loaded) {
    lderr(m_image_ctx->cct) << "encryption already loaded" << dendl;
    finish(-EEXIST);
    return;
  }

  auto ictx = m_image_ctx;
  while (ictx != nullptr) {
    if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
      lderr(m_image_ctx->cct) << "cannot use encryption with journal."
                              << " image name: " << ictx->name << dendl;
      finish(-ENOTSUP);
      return;
    }
    ictx = ictx->parent;
  }

  m_current_image_ctx = m_image_ctx;
  load();
}

template <typename I>
void LoadRequest<I>::load() {
  m_detected_format_name = "";
  ldout(m_image_ctx->cct, 20) << "format_idx=" << m_format_idx << dendl;
  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_load>(this);
  m_formats[m_format_idx]->load(m_current_image_ctx, &m_detected_format_name,
                                ctx);
}

template <typename I>
void LoadRequest<I>::handle_load(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    if (m_is_current_format_cloned && m_detected_format_name == "unknown") {
      // encryption format was not detected, assume plaintext
      ldout(m_image_ctx->cct, 5) << "assuming plaintext for image "
                                 << m_current_image_ctx->name << dendl;
      m_formats.pop_back();
      finish(0);
      return;
    }

    lderr(m_image_ctx->cct) << "failed to load encryption. image name: "
                            << m_current_image_ctx->name << dendl;
    finish(r);
    return;
  }

  ldout(m_image_ctx->cct, 5) << "loaded format " << m_detected_format_name
                             << (m_is_current_format_cloned ? " (cloned)" : "")
                             << " for image " << m_current_image_ctx->name
                             << dendl;

  m_format_idx++;
  m_current_image_ctx = m_current_image_ctx->parent;
  if (m_current_image_ctx != nullptr) {
    // move on to loading parent
    if (m_format_idx >= m_formats.size()) {
      // try to load next ancestor using the same format
      ldout(m_image_ctx->cct, 20) << "cloning format" << dendl;
      m_formats.push_back(m_formats[m_formats.size() - 1]->clone());
      m_is_current_format_cloned = true;
    }

    load();
  } else {
    if (m_formats.size() > m_format_idx) {
      lderr(m_image_ctx->cct) << "got " << m_formats.size()
                              << " encryption specs to load, "
                              << "but image has " << m_format_idx - 1
                              << " ancestors" << dendl;
      r = -EINVAL;
    }

    finish(r);
  }
}

template <typename I>
void LoadRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    auto ictx = m_image_ctx;
    for (auto& format : m_formats) {
      util::set_crypto(ictx, std::move(format));
      ictx = ictx->parent;
    }
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::LoadRequest<librbd::ImageCtx>;
